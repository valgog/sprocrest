package database

import play.api.Play.current
import play.api.db.slick.DB
import play.api.libs.json.Json

import scala.slick.jdbc.GetResult
import scala.slick.jdbc.StaticQuery.interpolation

object StoredProcedureTypes {
  type OID = Long
  type Namespace = String
  type Name = String
}

import database.StoredProcedureTypes._

case class Argument(name: Name, typeOid: OID, hasDefault: Boolean, mode: String)

object Argument {
  implicit val format = Json.format[Argument]
}

case class Attribute(name: Name, typeOid: OID)

object Attribute {
  implicit val format = Json.format[Attribute]
}

case class StoredProcedure(namespace: Namespace, name: Name, oid: OID, arguments: Seq[Argument] = Seq.empty)

object StoredProcedure {
  implicit val format = Json.format[StoredProcedure]
}


case class DbType(namespace: Namespace, name: Name, typeOid: OID, arrayOid: Option[OID],
                  typeChar: String, attributes: Seq[Attribute] = Seq.empty)

object DbType {
  implicit val format = Json.format[DbType]
}

object StoredProcedures {
  def buildTypes(): Map[OID, DbType] = {
    DB.withSession { implicit session =>

      // this is a seq of all the fields of complex types
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

      // all types known to the system
      val dbTypes: Seq[DbType] = {
        sql"""SELECT t.oid AS type_oid, nspname, typname, typarray, typtype
                FROM pg_type AS t, pg_namespace AS n
               WHERE t.typnamespace = n.oid""".as[(OID, Namespace, Name, OID, String)].list.toSeq.map {
          case (typeOid, namespace, name, arrayOid, typeChar) =>
            DbType(namespace = namespace, name = name, typeOid = typeOid,
              arrayOid = if (arrayOid == (0: OID)) None else Some(arrayOid), typeChar)
        }
      }

      dbTypes.groupBy(_.typeOid).mapValues { values =>
        require(values.size == 1, s"Found duplicate OID: $values")
        val dbType = values.head
        val attrTuples: Seq[(OID, Name, Int, OID)] = attributesByOwner.getOrElse(dbType.typeOid, Seq.empty)
        val attrs = attrTuples.map {
          case (parentOid, name, pos, typeOid) => Attribute(name, typeOid)
        }
        dbType.copy(attributes = attrs)
      }
    }
  }

  def buildStoredProcedures(): Map[(Namespace, Name), Seq[StoredProcedure]] = {
    DB.withSession { implicit session =>

      // this is a seq of all the fields of complex types
      val argumentsByProcOID: Map[OID, Seq[Argument]] = {
        sql"""SELECT prooid,
                     row_number() OVER w AS position,
                     row_number() OVER w > count(1) OVER w - pronargdefaults AS has_default,
                     COALESCE(proargmodes[i], 'i') AS param_mode,
                     proargnames[i] AS param_name,
                     CASE WHEN proallargtypes IS NULL THEN proargtypes[i-1] ELSE proallargtypes[i] END AS param_type_oid
                FROM (SELECT generate_subscripts(COALESCE(proallargtypes, proargtypes::oid[]), 1) + CASE WHEN proallargtypes IS NULL THEN 1 ELSE 0 END AS i,
                             oid as prooid,
                             proargnames,
                             proallargtypes,
                             proargtypes::oid[],
                             proargmodes,
                             pronargdefaults
                        FROM pg_proc
                       WHERE NOT proisagg
                         AND NOT proiswindow
                     ) a
               WHERE proargmodes IS NULL OR proargmodes[i] NOT IN('o', 't')
              WINDOW w AS (PARTITION BY prooid ORDER BY i)""".as[(OID, Int, Boolean, String, Name, OID)].list.toSeq.
          groupBy(_._1).
          mapValues(v => v.sortBy(_._4)).
          mapValues { values =>
          values.map {
            case (_, _, hasDefault, mode, name, typeOID) =>
              Argument(name, typeOID, hasDefault, mode)
          }
        }
      }

      sql"""SELECT p.oid AS proc_oid,
                   nspname,
                   proname
              FROM pg_proc AS p
              JOIN pg_namespace AS n ON p.pronamespace = n.oid
             WHERE NOT proisagg
               AND NOT proiswindow""".as[(OID, Namespace, Name)].list.toSeq.map {
        case (procOID, procNamespace, procName) =>
          StoredProcedure(procNamespace, procName, procOID,
            argumentsByProcOID.getOrElse(procOID, Seq.empty)
          )
      } groupBy (v => (v.namespace, v.name))
    }
  }
}
