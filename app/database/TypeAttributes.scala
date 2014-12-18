package database

import play.api.Play.current
import play.api.db.slick.DB
import play.api.libs.json.Json

import scala.slick.jdbc.GetResult
import scala.slick.jdbc.StaticQuery._

case class TypeAttribute(owningTypeOid: Long, attributeName: String, attributePosition: Int, attributeTypeOid: Long)

object TypeAttribute {
  implicit val getResult = GetResult[TypeAttribute] { r => TypeAttribute(r.<<, r.<<, r.<<, r.<<)}
  implicit val format = Json.format[TypeAttribute]
}

object TypeAttributes {

  val query = sql"""
SELECT c.reltype AS owning_type_oid,
       a.attname AS attribute_name,
       a.attnum AS attribute_position,
       a.atttypid AS attribute_type_oid
  FROM pg_class c
  JOIN pg_attribute a ON a.attrelid = c.oid
 WHERE a.attnum > 0
   AND NOT a.attisdropped
   AND c.relkind = ANY('{rvmcf}'::"char"[])
 ORDER BY owning_type_oid, attribute_position;
  """

  def loadTypes(): Seq[TypeAttribute] = {
    DB.withSession { implicit session =>
      query.as[TypeAttribute].list
    }
  }
}
