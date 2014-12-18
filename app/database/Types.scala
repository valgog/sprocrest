package database

import java.sql.Timestamp

import org.joda.time.format.ISODateTimeFormat
import play.api.Play.current
import play.api.db.slick.DB
import play.api.libs.json.{JsString, JsNumber, JsValue, Json}

import scala.slick.driver.PostgresDriver.simple._


case class TypeRecord(namespace: String, name: String, arrayType: Option[Int], `type`: String)

object TypeRecord {
  implicit val format = Json.format[TypeRecord]
}

case class Type(oid: Long, name: String, namespace: Int, `type`: String, array: Int)


class Types(tag: Tag) extends Table[Type](tag, "pg_type") {

  def oid = column[Long]("oid")

  def name = column[String]("typname")

  def namespace = column[Int]("typnamespace")

  def `type` = column[String]("typtype")

  def array = column[Int]("typarray")

  def * = (oid, name, namespace, `type`, array) <>(Type.tupled, Type.unapply)
}


object Types {
  def getTypeOf(i: Int) = getTypes().get(i.toString)

  lazy val pgtypes = TableQuery[Types]

  import database.Namespaces.pgnamespaces

  def getTypes(): Map[String, TypeRecord] = {
    DB.withSession { implicit session =>
      val query = for {
        t <- pgtypes
        n <- pgnamespaces if t.namespace === n.oid 
      } yield {
        (t.oid, n.name, t.name, t.array, t.`type`)
      }
      println(query.selectStatement)
      query.run.toIndexedSeq.map {
        case (oid, schema, name, array, typ) =>
          oid.toString -> TypeRecord(schema, name, if (array == 0) None else Some(array), typ)
      }.toMap
    }
  }

  def sqlConverter(argType: Long): JsValue => AnyRef = {
    import ISODateTimeFormat.dateTime
    getTypes().get(argType.toString).map(_.name).map {
      case "int2" => (value: JsValue) => value match {
        case number: JsNumber => number.value.toShort.asInstanceOf[AnyRef]
        case other => sys.error(s"Cannot construct an int2 from $other")
      }
      case "int4" => (value: JsValue) => value match {
        case number: JsNumber => number.value.toInt.asInstanceOf[AnyRef]
        case other => sys.error(s"Cannot construct an int4 from $other")
      }
      case "oid" | "int8" => (value: JsValue) => value match {
        case number: JsNumber => number.value.toLong.asInstanceOf[AnyRef]
        case other => sys.error(s"Cannot construct an int8 from $other")
      }
      case "numeric"|"decimal" => (value: JsValue) => value match {
        case number: JsNumber => number.value
        case other => sys.error(s"Cannot construct an double from $other")
      }
      case "float4" => (value: JsValue) => value match {
        case number: JsNumber => number.value.toFloat.asInstanceOf[AnyRef]
        case other => sys.error(s"Cannot construct a float from $other")
      }
      case "float8"|"money" => (value: JsValue) => value match {
        case number: JsNumber => number.value.toDouble.asInstanceOf[AnyRef]
        case other => sys.error(s"Cannot construct a double from $other")
      }
      case "bpchar" => (value: JsValue) => value match {
        case number: JsNumber => number.value.toChar.asInstanceOf[AnyRef]
        case other => sys.error(s"Cannot construct a char from $other")
      }
      case "varchar"|"text"|"name" => (value: JsValue) => value match {
        case number: JsString => number.value
        case other => sys.error(s"Cannot construct a string from $other")
      }
      case "time" | "timetz" => (value: JsValue) => value match {
        case number: JsString => new java.sql.Date(dateTime.parseDateTime(number.value).toDate.getTime)
        case other => sys.error(s"Cannot construct a string from $other")
      }
      case "timestamp" | "timestamptz" => (value: JsValue) => value match {
        case number: JsString => new Timestamp(dateTime.parseDateTime(number.value).toDate.getTime)
        case other => sys.error(s"Cannot construct a string from $other")
      }
    }.getOrElse(sys.error(s"Unknown type $argType"))
  }
}


case class NamespaceHolder(oid: Int, name: String)

class Namespaces(tag: Tag) extends Table[NamespaceHolder](tag, "pg_namespace") {

  def oid = column[Int]("oid")

  def name = column[String]("nspname")

  def * = (oid, name) <>(NamespaceHolder.tupled, NamespaceHolder.unapply)
}

object Namespaces {
  lazy val pgnamespaces = TableQuery[Namespaces]
}
