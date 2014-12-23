package controllers

import database.StoredProcedureTypes.OID
import database._
import play.api.libs.json.{JsObject, JsValue, Json}
import play.api.mvc.{Action, Controller, Request, Result}

import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

object RootController extends Controller {

  def types() = Action {
    Ok(Json.toJson(StoredProcedures.types.get.map(kv => kv._1.toString -> kv._2)))
  }

  def typeOf(id: Long) = Action {
    val allTypes: Map[OID, DbType] = StoredProcedures.types.get
    allTypes.get(id: OID).fold(NotFound: Result) { typ =>
      Ok(Json.toJson(typ))
    }
  }

  def procs() = Action {
    Ok(Json.toJson(StoredProcedures.loadStoredProcedures().map(kv => kv._1.toString -> kv._2)))
  }

  def procForNamespace(namespace: String) = Action {
    Ok(Json.toJson(StoredProcedures.loadStoredProcedures().filter(_._1._1 == namespace).map(kv => kv._1.toString -> kv._2)))
  }

  def procEntry(namespace: String, name: String) = Action {
    Ok(Json.toJson(StoredProcedures.loadStoredProcedures().filter(_._1 ==(namespace, name)).map(kv => kv._1.toString -> kv._2)))
  }

  def call(namespace: String, name: String) = Action(parse.json) { implicit request: Request[JsValue] =>

    request.body match {
      case obj: JsObject =>
        implicit val types = StoredProcedures.loadTypes()
        val sprocs = StoredProcedures.storedProcedures.get.get((namespace, name))
        val possibleSps: Seq[StoredProcedure] = sprocs.map {
            // what stored procedures can process the given incoming argument names?
            _.filter { (sproc: StoredProcedure) =>
              sproc.matches(obj.value.keys.toSet)
            }
        }.getOrElse(Seq.empty)

        possibleSps.toList match {
          case Nil =>
            if (sprocs.isEmpty) NotFound(views.json.error(s"Could not find a sproc $namespace.$name(...) for ${request.body}"))
            else BadRequest(s"sproc $namespace/$name exists, but could not match arguments; see /procs/$namespace/$name")
          case storedProcedure :: Nil =>
            val spArgs: Seq[Argument] = storedProcedure.arguments.getOrElse(Seq.empty)
            val inArgs: Map[String, JsValue] = obj.value.toMap

            {
              for {
                sqlArgs <- Try {
                  spArgs.collect {
                    case spArg if inArgs.contains(spArg.name) =>
                      val converter = StoredProcedures.sqlConverter(spArg.typeOid)
                      converter(inArgs(spArg.name))
                  }
                }
              } yield storedProcedure.execute(sqlArgs)
            } match {
              case Success(seq) => Ok(Json.parse(seq.mkString("[", ",", "]")))
              case Failure(e) => InternalServerError(views.json.error(e.toString))
            }
          case _ => BadRequest(views.json.error(s"Found multiple possible sprocs: $possibleSps"))
        }
      case other => BadRequest(views.json.error(s"Expected a json object, found: $other"))
    }
  }
}


