package controllers

import database.CustomTypes.{Name, Namespace, OID}
import database._
import play.api.libs.iteratee.Enumerator
import play.api.libs.json._
import play.api.mvc._

import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

object RootController extends Controller {

  import controllers.Serializers._

  private def withTypes[A](databaseName: String)
                          (f: Map[OID, DbType] => Either[String, A])
                          (implicit writer: Writes[A]): Result = {
    val typeMap: Map[Database, Map[OID, DbType]] = StoredProcedures.types.get
    val database = Database.byName(databaseName)
    typeMap.get(database).fold(NotFound(s"Database configuration for $databaseName not found"): Result) {
      m => f(m) match {
        case Right(r) => Ok(Json.toJson(r))
        case Left(msg) => NotFound(msg)
      }
    }
  }

  private def withStoredProcedures[A](databaseName: String)
                                     (f: Map[(Namespace, Name), Seq[StoredProcedure]] => A)
                                     (implicit writer: Writes[A]): Result = {
    val procMap: Map[Database, Map[(Namespace, Name), Seq[StoredProcedure]]] = StoredProcedures.storedProcedures.get()
    val database = Database.byName(databaseName)
    procMap.get(database).fold[Result](NotFound(s"Database configuration for $databaseName not found"))(m => Ok(Json.toJson(f(m))))
  }

  def databases() = Action {
    Ok(Json.toJson(Database.configuredDatabases))
  }

  def types() = Action {
    Ok(Json.toJson(StoredProcedures.types.get))
  }

  def typesForDatabase(database: String) = Action {
    withTypes(database) {
      types => Right(types)
    }
  }

  def typeOf(database: String, id: Long) = Action {
    withTypes(database) {
      _.get(id: OID).fold[Either[String, DbType]](Left(s"Type with OID $id not found in database $database"))(Right.apply)
    }
  }

  def procs() = Action {
    Ok(Json.toJson(StoredProcedures.storedProcedures.get))
  }

  def procsForDatabase(database: String) = Action {
    withStoredProcedures(database)(identity)
  }

  def procsForNamespace(database: String, namespace: String) = Action {
    withStoredProcedures(database)(_.filterKeys(_._1 == namespace))
  }

  def procEntry(database: String, namespace: String, name: String) = Action {
    withStoredProcedures(database)(_.filterKeys(_ ==(namespace, name)))
  }

  def call(database: String, namespace: String, name: String) = Action(parse.json) { implicit request: Request[JsValue] =>

    request.body match {
      case obj: JsObject =>
        implicit val db = Database.byName(database)
        implicit val types = StoredProcedures.types.get()(db)

        val snd = StoredProcedures.storedProcedures.get
        val sprocs = snd(Database.byName(database)).get((namespace, name))
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
              case Success(seq) =>
                Result(
                  ResponseHeader(200, Map(CONTENT_TYPE -> "application/json")),
                  Enumerator(seq.mkString("[", ",", "]").getBytes("UTF-8"))
                )
              case Failure(e) => InternalServerError(views.json.error(e.toString))
            }
          case _ => BadRequest(views.json.error(s"Found multiple possible sprocs: $possibleSps"))
        }
      case other => BadRequest(views.json.error(s"Expected a json object, found: $other"))
    }
  }
}


