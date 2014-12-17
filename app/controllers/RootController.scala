package controllers

import database.{Catalog, PgTypes}
import de.zalando.typemapper.postgres.PgTypeHelper
import play.api.db.slick.DBAction
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller, Result}

import scala.language.reflectiveCalls
import scala.slick.jdbc.GetResult
import scala.util.{Failure, Success, Try}

object RootController extends Controller {

  def catalog() = Action {
    Ok(Json.toJson(Catalog.catalog()))
  }

  def catalogEntry(schema: String, name: String) = Action {
    Catalog.catalogEntry(s"$schema/$name").fold(NotFound: Result) { entry =>
      Ok(Json.toJson(entry))
    }
  }

  def types() = Action {
    Ok(Json.toJson(PgTypes.getTypes()))
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

}


