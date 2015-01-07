package controllers

import database.CustomTypes.{Name, Namespace, OID}
import database._
import play.api.libs.json.{JsObject, JsValue, Json}
import play.api.mvc._

import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

object RootController extends Controller {

//  @inline final def forDatabaseName[K, V, M <: Map[K,V], A <: Action](databaseName: String, m: Map[Database, M])(f: M => A): A = {
//    val database = Database.byName(databaseName)
//    m.get(database).fold(NotFound(s"Database configuration for $databaseName not found")) {
//      element: M => f(element)
//    }
//  }

  def databases() = Action {
    Ok(Json.toJson(Database.configuredDatabases))
  }

  def types() = Action {
    val tid = StoredProcedures.types.get
    Ok(Json.toJson(o = tid map { case (db, ti) => db.name.toString -> ti.map{case (oid, t) => oid.toString -> t} } ))
  }

  def typesForDatabase(database: String) = Action {
    val tid = StoredProcedures.types.get

    tid.get(Database.byName(database)).fold(NotFound(s"Database configuration for $database not found")) {
      ti => Ok(Json.toJson(ti map { case (oid, t) => oid.toString -> t } ))
    }
  }

  def typeOf(database: String, id: Long) = Action {
    // Types per OID per database
    val tid = StoredProcedures.types.get

    tid.get(Database.byName(database)).fold(NotFound(s"Database configuration for $database not found")) {
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

    snd.get(Database.byName(database)).fold(NotFound(s"Database configuration for $database not found")) {
      // Stored procedures per name
      sn => Ok(Json.toJson(sn map { case (name, sproc) => name.toString -> sproc }))
    }
  }

  def procsForNamespace(database: String, namespace: String) = Action {
    // Stored procedures per name per database
    val snd = StoredProcedures.storedProcedures.get

    snd.get(Database.byName(database)).fold(NotFound(s"Database configuration for $database not found")) {
      // Stored procedures per name
      sn => Ok(Json.toJson(sn filterKeys ( _._1 == namespace ) map { case (name, sproc) => name.toString -> sproc }))
    }
  }

  def procEntry(database: String, namespace: String, name: String) = Action {
    // Stored procedures per name per database
    val snd = StoredProcedures.storedProcedures.get

    snd.get(Database.byName(database)).fold(NotFound(s"Database configuration for $database not found")) {
      // Stored procedures per name
      sn => Ok(Json.toJson(sn filterKeys ( _ == (namespace, name) ) map { case (name, sproc) => name.toString -> sproc }))
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


