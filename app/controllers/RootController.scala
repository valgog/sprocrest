package controllers

import database.StoredProcedureTypes.OID
import database._
import play.api.db.slick.DBAction
import play.api.libs.json.{JsObject, JsValue, Json}
import play.api.mvc.{Action, Controller, Result}

import scala.collection.mutable.ListBuffer
import scala.language.reflectiveCalls

object RootController extends Controller {

  def types() = Action {
    Ok(Json.toJson(StoredProcedures.loadTypes().map(kv => kv._1.toString -> kv._2)))
  }

  def typeOf(id: Long) = Action {
    val allTypes: Map[OID, DbType] = StoredProcedures.loadTypes()
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

  def arguments() = Action {
    Ok(Json.toJson(Arguments.loadArgDescriptions()))
  }

  def call(namespace: String, name: String) = DBAction(parse.json) { implicit rs =>
    implicit val session = rs.dbSession
    import rs.request

    request.body match {
      case obj: JsObject =>
        implicit val types = StoredProcedures.loadTypes()
        StoredProcedures.loadStoredProcedures().get((namespace, name)).map { spSeq =>
          val inArgNames = obj.value.keys.toSet
          val possibleSps = spSeq.filter { (sp: StoredProcedure) =>
            // 2 things must be true: every named input argument must exist in the args for this method AND
            // every function argument not present in the input must support a default value

            val spArgNames = sp.arguments.map { seq =>
              seq.map(_.name).toSet
            }.getOrElse(Set.empty)

            val spArgsMustHaveDefault = spArgNames.diff(inArgNames)

            val spArgsMissingWithDefaults = sp.arguments.map { arguments =>
              (for (argument <- arguments if spArgsMustHaveDefault(argument.name) && !argument.hasDefault) yield argument.name).toSet
            }.getOrElse(Set.empty)

            spArgNames.intersect(inArgNames) == inArgNames && spArgsMissingWithDefaults.isEmpty
          }
          if (possibleSps.isEmpty) BadRequest(views.json.error(s"Could not find a sproc for ${request.body}"))
          else if (possibleSps.size > 1) BadRequest(views.json.error(s"Found multiple possible sprocs: $possibleSps"))
          else {
            val storedProcedure: StoredProcedure = possibleSps.head
            val spArgs: Seq[Argument] = storedProcedure.arguments.get
            val inArgs: collection.Map[String, JsValue] = obj.value
            val sqlArgs = spArgs.collect {
              case spArg if inArgs.contains(spArg.name) =>
                val converter = StoredProcedures.sqlConverter(spArg.typeOid)
                converter(inArgs(spArg.name))
            }

            val preparedStatement = s"""
                         |SELECT to_json((f.*))
                         |  FROM $name(${(1 to sqlArgs.size).map(_ => "?").mkString(",")})
                         |    AS f
                        """.stripMargin

            import resource._

            val query = for {
              searchPathStatement <- managed(session.createStatement())
              _ = searchPathStatement.execute(s"set search_path to $namespace, PUBLIC")
              statement <- managed(session.prepareStatement(preparedStatement))
              _ = sqlArgs.zipWithIndex.foreach { case (arg, idx) => statement.setObject(idx + 1, arg: Object)}
              resultSet <- managed(statement.executeQuery())
            } yield {
              val buffer = new ListBuffer[String]
              while (resultSet.next()) {
                buffer += resultSet.getObject(1).toString
              }
              buffer.toSeq
            }

            query.map(identity).either match {
              case Right(seq: Seq[_]) => Ok(Json.parse(seq.mkString("[", ",", "]")))
              case Left(e) =>
                e.foreach(_.printStackTrace)
                InternalServerError(views.json.error(e.map(_.toString): _*))
            }
          }
        }.getOrElse(NotFound)
      case other => BadRequest(views.json.error(s"Expected a json object, found: $other"))

    }
  }

}


