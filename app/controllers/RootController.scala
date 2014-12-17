package controllers

import database.{Types, Procs}
import de.zalando.typemapper.postgres.PgTypeHelper
import play.api.db.slick.DBAction
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller, Result}

import scala.language.reflectiveCalls
import scala.slick.jdbc.{GetResult, PositionedResult}
import scala.util.{Failure, Success, Try}

case class ProcDesc(argTypes: Array[Long], argNames: Array[String]) {
  def args: Seq[(String, Long)] = argNames.zip(argTypes).init
}

object ProcDesc {
  implicit val fmt = Json.format[ProcDesc]
}

object RootController extends Controller {

  def procs() = Action {
    Ok(Json.toJson(Procs.procs()))
  }

  def procEntry(namespace: String, name: String) = Action {
    Procs.procEntry(s"$namespace/$name").fold(NotFound: Result) { entry =>
      Ok(Json.toJson(entry))
    }
  }

  def procForNamespace(namespace: String) = Action {
    Ok(Json.toJson(Procs.procsForNamespace(namespace)))
  }

  def types() = Action {
    Ok(Json.toJson(Types.getTypes()))
  }

  def typeOf(id: Int) = Action {
    Types.getTypeOf(id).fold(NotFound: Result) { typ =>
      Ok(Json.toJson(typ))
    }
  }

  def get() = DBAction { implicit rs =>
    implicit val session = rs.dbSession

    import scala.slick.jdbc.StaticQuery._


    Try {
      sql"SET search_path TO test_api, PUBLIC".as[Boolean].list
      implicit val getResult = GetResult(_.nextObject())
      val objects = sql"SELECT to_json((f.*)) FROM get_orders('00000001', 0) AS f".as[Object].list
      Ok(Json.parse(objects.mkString("[", ",", "]")))
    } match {
      case Success(ok) => ok
      case Failure(e) => InternalServerError(views.json.error(e.toString))
    }
  }

  def serialize() = DBAction { implicit rs =>
    Ok(PgTypeHelper.toPgString(Array[String]("hello", "world")))
  }


  def call(namespace: String, name: String) = DBAction(parse.json) { implicit rs =>
    implicit val session = rs.dbSession
    import scala.slick.jdbc.StaticQuery.interpolation
    import rs.request

    import java.sql
    val query = sql"""
        SELECT proallargtypes, proargnames
         FROM pg_proc JOIN pg_namespace AS ns ON ns.oid = pronamespace
        WHERE proname = $name
          AND ns.nspname = $namespace"""

    implicit val getter = GetResult { iter =>
      (iter.nextObject().asInstanceOf[sql.Array], iter.nextObject().asInstanceOf[sql.Array])
    }

    query.as[(sql.Array, sql.Array)].list.headOption.map {
      case (longs, strings) =>
        val args = ProcDesc(
          longs.getArray.asInstanceOf[Array[java.lang.Long]].map(_.longValue),
          strings.getArray.asInstanceOf[Array[String]]
        ).args

        val sqlArgs: Seq[AnyRef] = args.map {
          case (argName, argType: Long) =>
            Types.sqlConverter(argType)((request.body \\ argName).head)
        }
        println(s"sqlArgs = $sqlArgs")
        val preparedStatement =
          s"""
             |SELECT to_json((f.*))
             |  FROM $name(${(1 to sqlArgs.size).map(_ => "?").mkString(",")})
             |    AS f
            """.stripMargin
        println(s"preparedStatement = $preparedStatement")
        val pathStatement = session.createStatement()
        val pathResult = pathStatement.execute(s"SET search_path to $namespace, PUBLIC")
        val statement = session.prepareStatement(preparedStatement)
        sqlArgs.zipWithIndex.foreach {
          case (arg: AnyRef, idx: Int) =>
            statement.setObject(idx + 1, arg: Object)
        }
        val rs = statement.executeQuery()

        println(s"rs = $rs")
        while (rs.next()) {
          println(s"result = ${rs.getObject(1)}")
        }

        Ok(Json.toJson("foo"))
    }.getOrElse(NotFound)

  }

}


