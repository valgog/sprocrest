package database

import play.api.db.slick.DB
import play.api.Play.current
import play.api.libs.json.Json
import scala.slick.jdbc.StaticQuery.interpolation
import scala.slick.jdbc.{GetResult, PositionedResult}

object StoredProcedureTypes {
  type OID = Long
  type Namespace = String
  type Name = String
  type Path = (String, String)
}

import database.StoredProcedureTypes._

case class StoredProcedure(namespace: Namespace, name: Name, oid: OID, arguments: Seq[Argument])

case class Argument(name: Name, typeOid: OID)

case class Attribute(name: Name, typeOid: OID)

case class DbType(namespace: Namespace, name: Name, typeOid: OID, arrayOid: Option[OID],
                  typeChar: String, attributes: Seq[Attribute] = Seq.empty)

object StoredProcedures {
  def buildTypes(): Map[OID, DbType] = {
    DB.withSession { implicit session =>

      val attributes = sql"""

      SELECT c.reltype as owning_type_oid,
             a.attname as attribute_name,
             a.attnum as attribute_position,
             a.atttypid as attribute_type_oid
        FROM pg_class c
        JOIN pg_attribute a ON a.attrelid = c.oid
       WHERE a.attnum > 0
         AND NOT a.attisdropped
         AND c.relkind = ANY('{rvmcf}'::"char"[])
       ORDER BY owning_type_oid, attribute_position;

       """.as[(OID, Name, Int, OID)].list.toSeq

      val attributesByOid: Map[OID, Seq[(OID, Name, Int, OID)]] = attributes.groupBy(_._1).mapValues(v => v.sortBy(_._3))

      val typeRows = sql"""

      SELECT t.oid, nspname, typname, typarray, typtype
        FROM pg_type as t, pg_namespace as n where t.typnamespace = n.oid

    """.as[(OID, Namespace, Name, OID, String)].list.toSeq

      val dbTypes = typeRows.map {
        case (typeOid, namespace, name, arrayOid, typeChar) =>
          DbType(namespace = namespace, name = name, typeOid = typeOid,
            arrayOid = if (arrayOid == (0: OID)) None else Some(arrayOid), typeChar)
      }

      dbTypes.groupBy(_.typeOid).mapValues { values =>
        require(values.size == 1, s"Found duplicate OID: $values")
        val dbType = values.head
        val attrTuples: Seq[(OID, Name, Int, OID)] = attributesByOid(dbType.typeOid)
        val attrs = attrTuples.map {
          case (parentOid, name, pos, typeOid) => Attribute(name, typeOid)
        }
        dbType.copy(attributes = attrs)
      }
    }
  }

  def buildStoredProcedures(): Map[(Namespace, Name), StoredProcedure] = {

  }
}
