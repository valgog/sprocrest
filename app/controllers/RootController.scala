package controllers

import java.sql.{ResultSet, Statement}

import play.api.db.slick.DBAction
import play.api.libs.json.Json
import play.api.mvc.Controller

import scala.collection.immutable.IndexedSeq
import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

object RootController extends Controller {

  def resultStream(statement: Statement, resultSet: ResultSet): Stream[Seq[(String, AnyRef)]] = {
    val rsmd = resultSet.getMetaData
    val columns = rsmd.getColumnCount
    val labels = for (i <- 1 to columns) yield rsmd.getColumnName(i)

    def recurse(): Stream[Seq[(String, AnyRef)]] = {
      if (resultSet.next()) {
        val thisRow: IndexedSeq[(String, AnyRef)] = for (i <- 1 to columns) yield {
          labels(i - 1) -> resultSet.getObject(i)
        }
        thisRow.toSeq #:: recurse()
      } else {
        resultSet.close()
        statement.close()
        Stream.empty
      }
    }

    recurse()
  }

  def get() = DBAction { implicit rs =>
    import rs.dbSession.conn

    val rows = Try {
      val statement = conn.createStatement()
      statement.execute(
        """
          |CREATE OR REPLACE FUNCTION setoffunc() RETURNS SETOF int
          |AS 'SELECT 1 UNION SELECT 2;' LANGUAGE sql
          | """.stripMargin.trim)
      val resultSet = statement.executeQuery("SELECT * from setoffunc()")
      resultStream(statement, resultSet) map { row =>
        row.map {
          case (name, value) => name -> value.toString
        }
      }
    }

    rows match {
      case Success(seq: Stream[Seq[(String, String)]]) => Ok(Json.toJson(seq.map(_.toMap)))
      case Failure(error) => InternalServerError(views.json.error(unwrap(error).map(_.toString): _*))
    }
  }

  def unwrap(t: Throwable): List[Throwable] = t :: Option(t.getCause).map(unwrap).getOrElse(Nil)
}


