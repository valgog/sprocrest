package database

import play.api.db.slick.DB
import play.api.libs.json.Json

import scala.slick.jdbc.StaticQuery._
import scala.slick.jdbc.{GetResult, PositionedResult}

case class CatalogRow(specific_schema: String, specific_name: String, ordinal_position: Int, parameter_name: String,
                      formatted_type_name: String, procedure_oid: Int, unformatted_type_name: String, type_oid: Int)

object CatalogRow {
  implicit val mapper = GetResult[CatalogRow] {
    iter: PositionedResult =>
      CatalogRow(iter.nextString(), iter.nextString(), iter.nextInt(), iter.nextString(),
        iter.nextString(), iter.nextInt(), iter.nextString(), iter.nextInt())

  }
}

case class CatalogItem(position: Int, parameterName: String, formattedTypeName: String, procedureOid: Int,
                       unformattedTypeName: String, typeOid: Int)

object CatalogItem {
  implicit val format = Json.format[CatalogItem]

//  def apply(row: CatalogRow) = new CatalogItem(position = row.ordinal_position, parameterName = row.parameter_name,
//    formattedTypeName = row.formatted_type_name, procedureOid = row.procedure_oid,
//    unformattedTypeName = row.unformatted_type_name, typeOid = row.type_oid)
}

object Catalog {

  def catalog(): Map[String, CatalogItem] = {
    import play.api.Play.current

    DB.withSession { implicit session =>
      val rows = sql"""SELECT ss.n_nspname AS specific_schema,
                   ss.proname::TEXT AS specific_name,
                   (ss.x).n AS ordinal_position,
                   NULLIF(ss.proargnames[(ss.x).n], ''::TEXT) AS parameter_name,
                   CASE
                       WHEN t.typelem <> (0)::OID AND t.typlen = (-1)
                       THEN 'ARRAY'::TEXT
                       WHEN nt.nspname = 'pg_catalog'::NAME
                       THEN format_type(t.oid, NULL::INTEGER)
                       ELSE 'USER-DEFINED'::TEXT
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
                          information_schema._pg_expandarray(COALESCE(p.proallargtypes, (p.proargtypes)::OID[])) AS x
                     FROM pg_namespace n,
                          pg_proc p
                    WHERE ((n.oid = p.pronamespace)
                      AND (pg_has_role(p.proowner, 'USAGE'::TEXT) OR  has_function_privilege(p.oid, 'EXECUTE'::TEXT)))
                  ) ss
            WHERE t.oid = (ss.x).x
              AND t.typnamespace = nt.oid
              -- AND nt.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
              AND ss.proargmodes[(ss.x).n] = ANY ('{o,b,t}'::char[]);""".as[CatalogRow].list

      rows.map { row =>
        s"${row.specific_schema}/${row.specific_name}" -> new CatalogItem(position = row.ordinal_position, parameterName = row.parameter_name,
          formattedTypeName = row.formatted_type_name, procedureOid = row.procedure_oid,
          unformattedTypeName = row.unformatted_type_name, typeOid = row.type_oid)
      }.toMap
    }
  }
}
