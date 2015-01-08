package database

import database.CustomTypes.OID
import de.zalando.typemapper.postgres.{PgRow, PgArray}
import org.joda.time.format.ISODateTimeFormat
import org.specs2.mutable._
import play.api.libs.json.{Json, JsArray, JsString, JsNumber}
import play.api.test.WithApplication

class StoredProcedureTest extends Specification {

  "The stored procedures code" should {
    "convert simple types from json to sql-friendly types" in new WithApplication {
      def dbType(name: String) = DbType("pg_catalog", name, 0, None, None, "", None)
      StoredProcedures.simpleTypeConverter(dbType("int2"))(JsNumber(10)) must_== "10".toShort
      StoredProcedures.simpleTypeConverter(dbType("int4"))(JsNumber(10)) must_== "10".toInt
      StoredProcedures.simpleTypeConverter(dbType("oid"))(JsNumber(10)) must_== "10".toLong
      StoredProcedures.simpleTypeConverter(dbType("int8"))(JsNumber(10)) must_== "10".toLong
      StoredProcedures.simpleTypeConverter(dbType("numeric"))(JsNumber(10)) must_== BigDecimal("10")
      StoredProcedures.simpleTypeConverter(dbType("float4"))(JsNumber(4)) must_== 4f
      StoredProcedures.simpleTypeConverter(dbType("float8"))(JsNumber(4)) must_== 4d
      StoredProcedures.simpleTypeConverter(dbType("money"))(JsNumber(4)) must_== 4d
      StoredProcedures.simpleTypeConverter(dbType("bpchar"))(JsNumber(32)) must_== ' '
      StoredProcedures.simpleTypeConverter(dbType("varchar"))(JsString("zalando")) must_== "zalando"
      StoredProcedures.simpleTypeConverter(dbType("text"))(JsString("zalando")) must_== "zalando"
      StoredProcedures.simpleTypeConverter(dbType("name"))(JsString("zalando")) must_== "zalando"

      val timestamp = "2014-12-19T09:51:55.000Z"
      import java.sql
      val now = ISODateTimeFormat.dateTime.parseDateTime(timestamp).toDate
      StoredProcedures.simpleTypeConverter(dbType("time"))(JsString(timestamp)) must_== new sql.Date(now.getTime)
      StoredProcedures.simpleTypeConverter(dbType("timetz"))(JsString(timestamp)) must_== new sql.Date(now.getTime)
      StoredProcedures.simpleTypeConverter(dbType("timestamp"))(JsString(timestamp)) must_== new sql.Timestamp(now.getTime)
      StoredProcedures.simpleTypeConverter(dbType("timestamptz"))(JsString(timestamp)) must_== new sql.Timestamp(now.getTime)
    }

    "convert array types from json to sql-friendly types" in new WithApplication {
      implicit val database = Database.configuredDatabases.head
      val table: Map[OID, DbType] = StoredProcedures.types.get()(database)
      def dbType(name: String) = DbType(namespace = "pg_catalog", name = name, typeOid = 0, arrayOid = None,
        containedType = table.find(_._2.name == "int2").map(_._2.typeOid), typeChar = "", attributes = None)
      import scala.collection.JavaConverters._
      val is = StoredProcedures.arrayTypeConverter(table)(dbType("_int2"))(JsArray(Seq(JsNumber(10))))
      val shouldBe = PgArray.ARRAY(Seq(10).map(_.toShort).asJava)
      is must_== shouldBe
    }

    "convert complex object types from json to sql-friendly types" in new WithApplication() {
      val obj = Json.parse {
        """
          |{
          |  "sku": "i'm a sku",
          |  "description": "i'm a description"
          |}
        """.stripMargin
      }
      implicit val database = Database.configuredDatabases.head
      val table: Map[OID, DbType] = StoredProcedures.types.get()(database)
      val dbType = table.values.collect {
        case dbt@DbType("test_api", "order_item", _, _, _, _, _) => dbt
      }.head
      val is = StoredProcedures.complexTypeConverter(table)(dbType)(obj)
      is must_== PgRow.ROW("i'm a sku", "i'm a description")
    }
  }
}
