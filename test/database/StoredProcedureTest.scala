package database

import org.joda.time.format.ISODateTimeFormat
import org.specs2.mutable._
import play.api.libs.json.{JsString, JsNumber}
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
  }
}
