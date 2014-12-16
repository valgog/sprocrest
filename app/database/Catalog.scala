package database

import play.api.db.DB
import play.api.db.slick.{Session, DBAction}
import play.api.libs.json.Json
import play.api.mvc.Controller

import scala.slick.jdbc.StaticQuery._
import scala.slick.jdbc.{GetResult, PositionedResult}

//case class CatalogRow(specific_schema: String, specific_name: String, ordinal_position: Int, parameter_name: String,
//                      formatted_type_name: String, procedure_oid: Int, unformatted_type_name: String, type_oid: Int)
//
//object CatalogRow {
//  implicit val mapper = GetResult[CatalogRow] {
//    iter: PositionedResult =>
//      CatalogRow(iter.nextString(), iter.nextString(), iter.nextInt(), iter.nextString(),
//        iter.nextString(), iter.nextInt(), iter.nextString(), iter.nextInt())
//
//  }
//  implicit val format = Json.format[CatalogRow]
//}

trait Catalog {
  self: Controller =>

  import play.api.Play.current

  def catalog() = DBAction { implicit sessionRequest =>
    implicit val session = sessionRequest.dbSession

    type CatalogRow = (String, String, Int, String, String, Int, String, Int)

    val result: List[CatalogRow] = DB.withTransaction { implicit connection =>
      sql"""SELECT ss.n_nspname AS specific_schema,
                   ss.proname::text AS specific_name,
                   (ss.x).n AS ordinal_position,
                   NULLIF(ss.proargnames[(ss.x).n], ''::text) AS parameter_name,
                   CASE
                       WHEN t.typelem <> (0)::oid AND t.typlen = (-1)
                       THEN 'ARRAY'::text
                       WHEN nt.nspname = 'pg_catalog'::name
                       THEN format_type(t.oid, NULL::INTEGER)
                       ELSE 'USER-DEFINED'::text
                   END AS formatted_type_name,
                   ss.p_oid AS procedure_oid,
                   t.typname AS unformatted_type_name,
                   t.oid AS type_oid
            FROM pg_type t,
                 pg_namespace nt,
                 (
                   SELECT n.nspname AS n_nspname,
                          p.proname,
                          p.oid AS p_oid,
                          p.proargnames,
                          p.proargmodes,
                          information_schema._pg_expandarray(COALESCE(p.proallargtypes, (p.proargtypes)::oid[])) AS x
                     FROM pg_namespace n,
                          pg_proc p
                    WHERE ((n.oid = p.pronamespace)
                      AND (pg_has_role(p.proowner, 'USAGE'::text) OR  has_function_privilege(p.oid, 'EXECUTE'::text)))
                  ) ss
            WHERE t.oid = (ss.x).x
              AND t.typnamespace = nt.oid
              AND ss.proargmodes[(ss.x).n] = ANY ('{o,b,t}'::char[]);""".as[CatalogRow].list
    }
    Ok(Json.toJson(result))
  }
}
