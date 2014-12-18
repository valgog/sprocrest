package database

import play.api.Play.current
import play.api.db.slick.DB
import play.api.libs.json.Json

import scala.slick.jdbc.GetResult
import scala.slick.jdbc.StaticQuery._

object Arguments {

  case class ArgRecord(namespace: String, procOid: Long, procName: String, position: Int, hasDefault: Boolean, paramMode: String,
                       paramName: String, paramOid: Long)

  object ArgRecord {
    implicit val getResult = GetResult {
      case result =>
        import result.{nextBoolean, nextInt, nextLong, nextString}
        ArgRecord(nextString(), nextLong(), nextString(), nextInt(), nextBoolean(), nextString(), nextString(), nextLong())
    }

    implicit val format = Json.format[ArgRecord]
  }

  val query = sql"""
    SELECT nspname AS namespace, prooid, proname,
             row_number() OVER w AS position,
             row_number() OVER w > count(1) OVER w - pronargdefaults AS has_default,
             COALESCE(proargmodes[i], 'i') AS param_mode,
             proargnames[i] AS param_name,
             CASE WHEN proallargtypes IS NULL THEN proargtypes[i-1] ELSE proallargtypes[i] END AS param_type_oid
     FROM (SELECT generate_subscripts(COALESCE(proallargtypes, proargtypes::OID[]), 1) + CASE WHEN proallargtypes IS NULL THEN 1 ELSE 0 END AS i,
              nspname, p.oid AS prooid,
              proname,
              proargnames,
              proallargtypes,
              proargtypes::OID[],
              proargmodes,
              pronargdefaults
     FROM pg_proc p
        JOIN pg_namespace n ON pronamespace=n.oid
     ) a WHERE proargmodes IS NULL OR proargmodes[i] NOT IN('o', 't') WINDOW w AS (PARTITION BY prooid ORDER BY i);
  """

  def loadArgDescriptions(): Seq[ArgRecord] = {
    DB.withSession { implicit session =>
      query.as[ArgRecord].list
    }
  }
}
