package database

import play.api.Play.current
import play.api.db.slick.DB
import play.api.libs.json.Json

import scala.slick.driver.PostgresDriver.simple._


case class TypeResult(oid: Int, namespace: String, name: String, arrayType: Option[Int], typ: String)

object TypeResult {
  implicit val format = Json.format[TypeResult]
}

case class PgType(oid: Int, name: String, namespace: Int, `type`: String, array: Int)


class PgTypes(tag: Tag) extends Table[PgType](tag, "pg_type") {

  def oid = column[Int]("oid")

  def name = column[String]("typname")

  def namespace = column[Int]("typnamespace")

  def `type` = column[String]("typtype")

  def array = column[Int]("typarray")

  def * = (oid, name, namespace, `type`, array) <>(PgType.tupled, PgType.unapply)
}


object PgTypes {
  lazy val pgtypes = TableQuery[PgTypes]

  import database.PgNamespaces.pgnamespaces

  def getTypes(): Seq[TypeResult] = {
    DB.withSession { implicit session =>
      val query = for {
        t <- pgtypes
        n <- pgnamespaces if t.namespace === n.oid && n.name =!= "pg_catalog" && n.name =!= "information_schema" && n.name =!= "pg_toast"
      } yield {
        (t.oid, n.name, t.name, t.array, t.`type`)
      }
      println(query.selectStatement)
      query.run.toIndexedSeq.map {
        case (oid, schema, name, array, typ) =>
          TypeResult(oid, schema, name, if (array == 0) None else Some(array), typ)
      }
    }
  }
}


case class PgNamespace(oid: Int, name: String)

class PgNamespaces(tag: Tag) extends Table[PgNamespace](tag, "pg_namespace") {

  def oid = column[Int]("oid")

  def name = column[String]("nspname")

  def * = (oid, name) <>(PgNamespace.tupled, PgNamespace.unapply)
}

object PgNamespaces {
  lazy val pgnamespaces = TableQuery[PgNamespaces]
}
