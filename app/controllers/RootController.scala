package controllers

import database.CustomTypes.{Name, Namespace, OID}
import database._
import play.api.libs.json.{JsObject, JsValue, Json}
import play.api.mvc._

import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

object RootController extends Controller {

  def withTypes(databaseName: String)(f: Map[OID, DbType] => Result): Result = {
    val typeMap = StoredProcedures.types.get
    val database = Database.byName(databaseName)
    typeMap.get(database).fold(NotFound(s"Database configuration for $databaseName not found"): Result)(f)
  }

  def withStoredProcedures(databaseName: String)(f: Map[(Namespace, Name), Seq[StoredProcedure]] => Result): Result = {
    val procMap = StoredProcedures.storedProcedures.get()
    val database = Database.byName(databaseName)
    procMap.get(database).fold(NotFound(s"Database configuration for $databaseName not found"): Result)(f)
  }

  def databases() = Action {
    Ok(Json.toJson(Database.configuredDatabases))
  }

  def types() = Action {
    val tid: Map[Database, Map[OID, DbType]] = StoredProcedures.types.get
    Ok(Json.toJson(o = tid map { case (db, ti) => db.name.toString -> ti.map{case (oid, t) => oid.toString -> t} } ))
  }

  def typesForDatabase(database: String) = Action {
    withTypes(database) {
      types => Ok(Json.toJson(types.map(kv => kv._1.toString -> kv._2)))
    }
  }

  def typeOf(database: String, id: Long) = Action {
    withTypes(database) {
      ti => ti.get(id: OID).fold(NotFound(s"Type with OID $id not found in database $database")) {
        t => Ok(Json.toJson(t))
      }
    }
  }

  def procs() = Action {
    Ok(Json.toJson(StoredProcedures.storedProcedures.get.map { case (db, sn) => db.name.toString -> sn.map{case (n, s) => n.toString -> s} }))
  }

  def procsForDatabase(database: String) = Action {
    // Stored procedures per name per database
    val snd = StoredProcedures.storedProcedures.get

    withStoredProcedures(database) {
      sn => Ok(Json.toJson(sn.map(kv => kv._1.toString -> kv._2)))
    }
  }

  def procsForNamespace(database: String, namespace: String) = Action {
    // Stored procedures per name per database
    withStoredProcedures(database) {
      sn => Ok(Json.toJson(sn.filterKeys( _._1 == namespace ).map(kv => kv._1.toString -> kv._2)))
    }
  }

  def procEntry(database: String, namespace: String, name: String) = Action {
    // Stored procedures per name per database
    val snd = StoredProcedures.storedProcedures.get

    withStoredProcedures(database) {
      sn => Ok(Json.toJson(sn.filterKeys( _ == (namespace, name)).map(kv => kv._1.toString -> kv._2)))
    }
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
              case Success(seq) => Ok(Json.parse(seq.mkString("[", ",", "]")))
              case Failure(e) => InternalServerError(views.json.error(e.toString))
            }
          case _ => BadRequest(views.json.error(s"Found multiple possible sprocs: $possibleSps"))
        }
      case other => BadRequest(views.json.error(s"Expected a json object, found: $other"))
    }
  }
}


