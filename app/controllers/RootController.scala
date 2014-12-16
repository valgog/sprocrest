package controllers

import database.{Catalog, PgTypes}
import play.api.db.slick.DBAction
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}

import scala.language.reflectiveCalls
import scala.slick.jdbc.GetResult
import scala.util.{Failure, Success, Try}

object RootController extends Controller {

  def catalog() = Action {
    Ok(Json.toJson(Catalog.catalog()))
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
      val objects = sql"SELECT to_json((f.*)) FROM get_orders('00000001') AS f".as[Object].list
      Ok(Json.parse(objects.mkString("[", ",", "]")))
    } match {
      case Success(ok) => ok
      case Failure(e) => InternalServerError(views.json.error(e.toString))
    }
  }
}


