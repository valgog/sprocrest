package database

import org.joda.time.format.ISODateTimeFormat
import org.specs2.mutable._
import play.api.libs.json.{JsString, JsNumber}

class StoredProcedureTest extends Specification {

  "The stored procedures code" should {
    "convert simple types from json to sql-friendly types" in {
      StoredProcedures.simpleTypeConverter("int2")(JsNumber(10)) must_== "10".toShort
      StoredProcedures.simpleTypeConverter("int4")(JsNumber(10)) must_== "10".toInt
      StoredProcedures.simpleTypeConverter("oid")(JsNumber(10)) must_== "10".toLong
      StoredProcedures.simpleTypeConverter("int8")(JsNumber(10)) must_== "10".toLong
      StoredProcedures.simpleTypeConverter("numeric")(JsNumber(10)) must_== BigDecimal("10")
      StoredProcedures.simpleTypeConverter("decimal")(JsNumber(10)) must_== BigDecimal("10")
      StoredProcedures.simpleTypeConverter("float4")(JsNumber(4)) must_== 4f
      StoredProcedures.simpleTypeConverter("float8")(JsNumber(4)) must_== 4d
      StoredProcedures.simpleTypeConverter("money")(JsNumber(4)) must_== 4d
      StoredProcedures.simpleTypeConverter("bpchar")(JsNumber(32)) must_== ' '
      StoredProcedures.simpleTypeConverter("varchar")(JsString("zalando")) must_== "zalando"
      StoredProcedures.simpleTypeConverter("text")(JsString("zalando")) must_== "zalando"
      StoredProcedures.simpleTypeConverter("name")(JsString("zalando")) must_== "zalando"

      val timestamp = "2014-12-19T09:51:55.000Z"
      val now = ISODateTimeFormat.dateTime.parseDateTime(timestamp).toDate
      StoredProcedures.simpleTypeConverter("time")(JsString(timestamp)) must_== new java.sql.Date(now.getTime)
      StoredProcedures.simpleTypeConverter("timetz")(JsString(timestamp)) must_== new java.sql.Date(now.getTime)
      StoredProcedures.simpleTypeConverter("timestamp")(JsString(timestamp)) must_== new java.sql.Timestamp(now.getTime)
      StoredProcedures.simpleTypeConverter("timestamptz")(JsString(timestamp)) must_== new java.sql.Timestamp(now.getTime)
    }
  }
}
