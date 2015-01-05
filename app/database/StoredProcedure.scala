package database

import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicReference

import akka.actor.Props
import de.zalando.typemapper.postgres.PgArray
import org.joda.time.format.ISODateTimeFormat.dateTime
import play.api.Configuration
import play.api.Play.current
import play.api.db.slick.DB
import play.api.libs.json._

import scala.collection.mutable.ListBuffer
import scala.slick.jdbc.StaticQuery._

object CustomTypes {
  type ClusterName = String
  type DatabaseShardId = Int
  type OID = Long
  type Namespace = String
  type Name = String
}

import database.CustomTypes._

case class Database(name: Name, cluster: ClusterName, shardId: Option[DatabaseShardId] = None)

object Database {

  implicit val format = Json.format[Database]

  val databaseNamePattern = """^([^\d_]+)(\d+)?(?:_db)?$""".r("clusterName", "shardId")

  lazy val configuredDatabases: Set[Database] = {
    import play.api.Play.{current => app}
    val databaseConfig = app.configuration.getConfig("db").getOrElse(Configuration.empty)
    databaseConfig.subKeys.map {
      case name@databaseNamePattern(clusterName, shardId) => Database(name, clusterName, Option(shardId).map(_.toInt))
      case name => throw databaseConfig.reportError(
        s"db.$name", s"Database datasource names should consist of cluster name and optional shard id")
    }
  }

  lazy val databaseByName: Map[String, Database] = configuredDatabases.groupBy(_.name).mapValues(_.head)

  def byName(databaseName: String): Database = databaseByName(databaseName)
}

case class Argument(name: Name, typeOid: OID, hasDefault: Boolean, mode: String)

object Argument {
  implicit val format = Json.format[Argument]
}

case class Attribute(name: Name, typeOid: OID)

object Attribute {
  implicit val format = Json.format[Attribute]
}

case class MultipleException(exceptions: Seq[Throwable]) extends Exception {
  override def toString = exceptions.map(_.toString).mkString("[", ",", "]")
}

case class StoredProcedure(namespace: Namespace, name: Name, oid: OID, arguments: Option[Seq[Argument]]) {

  def matches(argNames: Set[String]): Boolean = {
    // set of the stored procedure argument names
    val spArgNames = arguments.map { seq =>
      seq.map(_.name).toSet
    }.getOrElse(Set.empty)

    println(s"stored procedure argument names are $spArgNames")

    // set of sp arguments that must have a default value for this call to succeed
    val spArgsMustHaveDefault = spArgNames.diff(argNames)

    // the set of sp args that don't have a default value, that are not present in the incoming args
    val spArgsMissingWithDefaults = arguments.map { arguments =>
      (for (argument <- arguments if spArgsMustHaveDefault(argument.name) && !argument.hasDefault) yield argument.name).toSet
    }.getOrElse(Set.empty)

    // for us to process this request, all the incoming arguments must be present in the sp arguments,
    // and there can't be any missing arguments that don't have a default value in the sproc
    ( spArgNames.isEmpty && argNames.isEmpty ) ||
      ( spArgNames.intersect(argNames) == argNames && spArgsMissingWithDefaults.isEmpty )
  }

  def execute(sqlArgs: Seq[AnyRef])(implicit db: Database): Iterable[String] = {
    DB(db.name).withSession { implicit session =>
      // somewhat hand-crafted sproc call, that returns json we stream directly back to the caller
      val preparedStatement = s"""
                         |SELECT to_json((f.*))
                         |  FROM $name(${(1 to sqlArgs.size).map(_ => "?").mkString(",")})
                         |    AS f
                        """.stripMargin

      import resource._

      val query = for {
        searchPathStatement <- managed(session.createStatement())
        _ = searchPathStatement.execute(s"set search_path to $namespace, PUBLIC")
        statement <-  managed(session.prepareStatement(preparedStatement))
        _ = sqlArgs.zipWithIndex.foreach { case (arg, idx) => statement.setObject(idx + 1, arg: Object)}
        resultSet <- managed(statement.executeQuery())
      } yield {
        // todo is there a nicer way to do this with slick?
        val buffer = new ListBuffer[String]
        while (resultSet.next()) {
          buffer += resultSet.getObject(1).toString
        }
        buffer.toSeq
      }

      query.map(identity).either match {
        case Left(throwables) => throw MultipleException(throwables)
        case Right(rows) => rows
      }
    }
  }
}

object StoredProcedure {
  implicit val format = Json.format[StoredProcedure]
}


case class DbType(namespace: Namespace, name: Name, typeOid: OID, arrayOid: Option[OID], containedType: Option[OID],
                  typeChar: String, attributes: Option[Seq[Attribute]] = None)

object DbType {
  implicit val format = Json.format[DbType]
}


object StoredProcedures {

  def start(): Unit = {
    import play.api.libs.concurrent.Akka.system
    val coordinator = system.actorOf(Props[PeriodicTaskSupervisor], name = "LoadingCoordinator")
    coordinator ! Props(classOf[BlockingPeriodicTask], "TypesLoader",
      () => StoredProcedures.types.set(StoredProcedures.loadTypes()))
    coordinator ! Props(classOf[BlockingPeriodicTask], "SprocLoader",
      () => StoredProcedures.storedProcedures.set(StoredProcedures.loadStoredProcedures()))
  }

  val types = new AtomicReference[Map[Database, Map[OID, DbType]]](loadTypes())
  val storedProcedures = new AtomicReference[Map[Database, Map[(Namespace, Name), Seq[StoredProcedure]]]](loadStoredProcedures())

  def loadTypes(): Map[Database, Map[OID, DbType]] = {
    Database.configuredDatabases.map { database =>
      database -> DB(database.name).withSession { implicit session =>

        // load all types known to the system
        val dbTypes: Seq[DbType] = {
          sql"""SELECT t.oid AS type_oid, nspname, typname, typarray, typtype
                FROM pg_type AS t, pg_namespace AS n
               WHERE t.typnamespace = n.oid""".as[(OID, Namespace, Name, OID, String)].list.toSeq.map {
            case (typeOid, namespace, name, arrayOid, typeChar) =>
              DbType(namespace = namespace, name = name, typeOid = typeOid,
                arrayOid = if (arrayOid == (0: OID)) None else Some(arrayOid), None, typeChar)
          }
        }

        // load all the attributes of complex types
        val attributesByOwner: Map[OID, Seq[(OID, Name, Int, OID)]] = {
          sql"""SELECT c.reltype AS owning_type_oid,
                     a.attname AS attribute_name,
                     a.attnum AS attribute_position,
                     a.atttypid AS attribute_type_oid
                FROM pg_class c
                JOIN pg_attribute a ON a.attrelid = c.oid
               WHERE a.attnum > 0
                 AND NOT a.attisdropped
                 AND c.relkind IN ('r', 'v', 'm', 'c', 'f')
               ORDER BY owning_type_oid, attribute_position""".as[(OID, Name, Int, OID)].
            list.toSeq.groupBy(_._1).
            mapValues(v => v.sortBy(_._3))
        }

        // populate complex types with their attributes
        val withAttributes: Seq[DbType] = dbTypes.map { dbType =>
          val attrTuples: Seq[(OID, Name, Int, OID)] = attributesByOwner.getOrElse(dbType.typeOid, Seq.empty)
          val attrs = attrTuples.map {
            case (parentOid, name, pos, typeOid) => Attribute(name, typeOid)
          }
          if (attrs.nonEmpty) dbType.copy(attributes = Some(attrs)) else dbType
        }

        // build a map of array oid -> type it contains
        val arrayOidToTypeOid: Map[OID, Option[OID]] =
          withAttributes.filter(_.arrayOid.isDefined).map(dbType => dbType.arrayOid.get -> Some(dbType.typeOid)).toMap

        // inject the reference to the contained type into each array type
        val typesWithArrays = dbTypes.map { dbType =>
          arrayOidToTypeOid.get(dbType.typeOid).map(contained => dbType.copy(containedType = contained)).getOrElse(dbType)
        }

        // group into a map so we can lookup types by oid
        val grouped: Map[OID, Seq[DbType]] = typesWithArrays.groupBy(_.typeOid)

        // reduce to a map of oid -> dbType, and make sure there is exactly 1 type for each OID
        grouped.map { case (oid, values) =>
          require(values.size == 1, s"Type $oid has multiple values: [$values]")
          oid -> values.head
        }
      }
    }.toMap
  }

  def loadStoredProcedures(): Map[Database, Map[(Namespace, Name), Seq[StoredProcedure]]] = {
    Database.configuredDatabases.map { database =>
      database -> DB(database.name).withSession { implicit session =>
        // build a map of procId to a list of that proc's arguments
        val argumentsByProcOID: Map[OID, Seq[Argument]] = {
          val rows =
            sql"""SELECT prooid,
                       row_number() OVER w AS position,
                       row_number() OVER w > count(1) OVER c - pronargdefaults AS has_default,
                       COALESCE(proargmodes[i], 'i') AS param_mode,
                       proargnames[i] AS param_name,
                       CASE WHEN proallargtypes IS NULL THEN proargtypes[i-1] ELSE proallargtypes[i] END AS param_type_oid
                  FROM (SELECT generate_subscripts(COALESCE(proallargtypes, proargtypes::OID[]), 1) + CASE WHEN proallargtypes IS NULL THEN 1 ELSE 0 END AS i,
                               oid AS prooid,
                               proargnames,
                               proallargtypes,
                               proargtypes::OID[],
                               proargmodes,
                               pronargdefaults
                          FROM pg_proc
                         WHERE NOT proisagg
                           AND NOT proiswindow
                       ) a
                 WHERE proargmodes IS NULL OR proargmodes[i] NOT IN('o', 't')
                WINDOW c AS (PARTITION BY prooid),
                       w AS (c ORDER BY i)""".as[(OID, Int, Boolean, String, Name, OID)].list.toSeq

          rows.groupBy(_._1).mapValues {
            _.sortBy(_._2).map {
              case (_, _, hasDefault, mode, name, typeOID) => Argument(name, typeOID, hasDefault, mode)
            }
          }
        }

        // load all stored procedures, and inject each ones arguments, if any
        sql"""SELECT p.oid AS proc_oid,
                   nspname,
                   proname
              FROM pg_proc AS p
              JOIN pg_namespace AS n ON p.pronamespace = n.oid
             WHERE NOT proisagg
               AND NOT proiswindow""".as[(OID, Namespace, Name)].list.toSeq.map {
          case (procOID, procNamespace, procName) =>
            StoredProcedure(procNamespace, procName, procOID, argumentsByProcOID.get(procOID).filter(_.nonEmpty))
        } groupBy (v => (v.namespace, v.name))
      }
    }.toMap
  }

  val simpleTypeConverter: PartialFunction[DbType, JsValue => AnyRef] = {
    case DbType("pg_catalog", "int2", _, _, _, _, _) => {
      case number: JsNumber => number.value.toShort.asInstanceOf[AnyRef]
      case other => sys.error(s"Cannot construct an int2 from $other")
    }
    case DbType("pg_catalog", "int4", _, _, _, _, _) => {
      case number: JsNumber => number.value.toInt.asInstanceOf[AnyRef]
      case other => sys.error(s"Cannot construct an int4 from $other")
    }
    case DbType("pg_catalog", "oid", _, _, _, _, _) | DbType("pg_catalog", "int8", _, _, _, _, _) => {
      case number: JsNumber => number.value.toLong.asInstanceOf[AnyRef]
      case other => sys.error(s"Cannot construct an int8 from $other")
    }
    case DbType("pg_catalog", "numeric", _, _, _, _, _) => (value: JsValue) => value match {
      case number: JsNumber => number.value
      case other => sys.error(s"Cannot construct an double from $other")
    }
    case DbType("pg_catalog", "float4", _, _, _, _, _) => {
      case number: JsNumber => number.value.toFloat.asInstanceOf[AnyRef]
      case other => sys.error(s"Cannot construct a float from $other")
    }
    case DbType("pg_catalog", "float8", _, _, _, _, _) | DbType("pg_catalog", "money", _, _, _, _, _) => {
      case number: JsNumber => number.value.toDouble.asInstanceOf[AnyRef]
      case other => sys.error(s"Cannot construct a double from $other")
    }
    case DbType("pg_catalog", "bpchar", _, _, _, _, _) => {
      case number: JsNumber => number.value.toChar.asInstanceOf[AnyRef]
      case other => sys.error(s"Cannot construct a char from $other")
    }
    case DbType("pg_catalog", "varchar", _, _, _, _, _) | DbType("pg_catalog", "text", _, _, _, _, _) | DbType("pg_catalog", "name", _, _, _, _, _) => {
      case number: JsString => number.value
      case other => sys.error(s"Cannot construct a string from $other")
    }
    case DbType("pg_catalog", "time", _, _, _, _, _) | DbType("pg_catalog", "timetz", _, _, _, _, _) => {
      case number: JsString => new java.sql.Date(dateTime.parseDateTime(number.value).toDate.getTime)
      case other => sys.error(s"Cannot construct a string from $other")
    }
    case DbType("pg_catalog", "timestamp", _, _, _, _, _) | DbType("pg_catalog", "timestamptz", _, _, _, _, _) => {
      case number: JsString => new Timestamp(dateTime.parseDateTime(number.value).toDate.getTime)
      case other => sys.error(s"Cannot construct a string from $other")
    }
  }

  def sqlConverter(argType: DbType)(implicit table: OID => DbType): JsValue => AnyRef = {

    if (simpleTypeConverter.isDefinedAt(argType)) {
      simpleTypeConverter(argType)
    } else if (argType.containedType.isDefined) {
      val containedType = table(argType.containedType.get)

      {
        case array: JsArray =>
          val converted: Seq[AnyRef] = array.value.map(simpleTypeConverter(containedType))
          import scala.collection.JavaConverters._
          PgArray.ARRAY(converted.asJava)
        case other => sys.error(s"Don't know how to convert $argType container of $containedType")
      }
    } else {
      sys.error(s"We don't handle complex types yet: $argType")
    }
  }
}
