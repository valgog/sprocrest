package database

import play.api.Play.current
import play.api.db.slick.DB
import play.api.libs.json.Json

import scala.collection.immutable
import scala.slick.jdbc.GetResult
import scala.slick.jdbc.StaticQuery.interpolation

object StoredProcedureTypes {
  type OID = Long
  type Namespace = String
  type Name = String
  type Path = (String, String)
}

import database.StoredProcedureTypes._

case class Argument(name: Name, typeOid: OID)

object Argument {
  implicit val format = Json.format[Argument]
}

case class Attribute(name: Name, typeOid: OID)

object Attribute {
  implicit val format = Json.format[Attribute]
}

case class StoredProcedure(namespace: Namespace, name: Name, oid: OID, arguments: Seq[Argument])

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
                 AND c.relkind = ANY('{rvmcf}'::"char"[])
               ORDER BY owning_type_oid, attribute_position;""".as[(OID, Name, Int, OID)] {
          GetResult[(OID, Name, Int, OID)] { case r => (r.<<, r.<<, r.<<, r.<<)}
        }.list.toSeq.groupBy(_._1).mapValues(v => v.sortBy(_._3))
      }

      // all types known to the system
      val dbTypes: Seq[DbType] = {
        sql"""SELECT t.oid, nspname, typname, typarray, typtype
                FROM pg_type AS t, pg_namespace AS n
               WHERE t.typnamespace = n.oid""".as[(OID, Namespace, Name, OID, String)] {
          GetResult[(OID, Namespace, Name, OID, String)] { case r => (r.<<, r.<<, r.<<, r.<<, r.<<)}
        }.list.toSeq.map {
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

  def buildStoredProcedures(): Map[(Namespace, Name), StoredProcedure] = {
    null
  }
}
