package controllers

import database.StoredProcedureTypes.OID
import database._
import de.zalando.typemapper.postgres.PgTypeHelper
import play.api.db.slick.DBAction
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller, Result}

import scala.collection.mutable.ListBuffer
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

  def types() = Action {
    Ok(Json.toJson(StoredProcedures.buildTypes().map(kv => kv._1.toString -> kv._2)))
  }

  def typeOf(id: Long) = Action {
    val allTypes: Map[OID, DbType] = StoredProcedures.buildTypes()
    allTypes.get(id: OID).fold(NotFound: Result) { typ =>
      Ok(Json.toJson(typ))
    }
  }

  def procs() = Action {
    Ok(Json.toJson(StoredProcedures.buildStoredProcedures().map(kv => kv._1.toString -> kv._2)))
  }

  def arguments() = Action {
    Ok(Json.toJson(Arguments.loadArgDescriptions()))
  }

  def procEntry(namespace: String, name: String) = Action {
    Procs.procEntry(s"$namespace/$name").fold(NotFound: Result) { entry =>
      Ok(Json.toJson(entry))
    }
  }

  def procForNamespace(namespace: String) = Action {
    Ok(Json.toJson(Procs.procsForNamespace(namespace)))
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
            StoredProcedures.sqlConverter(argType)((request.body \\ argName).head)
        }
        val preparedStatement =
          s"""
             |SELECT to_json((f.*))
             |  FROM $name(${(1 to sqlArgs.size).map(_ => "?").mkString(",")})
             |    AS f
            """.stripMargin
        import resource._
        val operation = for {
          pathStatement <- managed(session.createStatement())
          pathResult = pathStatement.execute(s"SET search_path to $namespace, PUBLIC")
          statement <- managed(session.prepareStatement(preparedStatement))
          _ = sqlArgs.zipWithIndex.foreach { case (arg , idx ) => statement.setObject(idx + 1, arg: Object) }
          resultSet <- managed(statement.executeQuery())
        } yield {
          val buffer = new ListBuffer[AnyRef]
          while (resultSet.next()) {
            buffer += resultSet.getObject(1)
          }
          buffer.toSeq
        }

        operation.map(identity).either match {
          case Right(seq: Seq[_]) => Ok(Json.parse(seq.mkString("[", ",", "]")))
          case Left(e) => InternalServerError(views.json.error(e.map(_.toString):_*))
        }
    }.getOrElse(NotFound)

  }

}


