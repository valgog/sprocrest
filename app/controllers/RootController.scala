package controllers

import java.io.StringWriter
import java.sql.{ResultSet, Statement}

import database.Catalog
import org.postgresql.util.PGobject
import play.api.db.slick.DBAction
import play.api.libs.json.Json
import play.api.mvc.Controller

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ListBuffer
import scala.language.reflectiveCalls
import scala.util.Try

object RootController extends Controller with Catalog {

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

  case class ResultSetIterator(rs: ResultSet) extends Iterator[PGobject] {
    override def hasNext: Boolean = rs.next()

    override def next(): PGobject = rs.getObject(1).asInstanceOf[PGobject]
  }

  def get() = DBAction { implicit rs =>
    import resource._
    import rs.dbSession.conn

    val result = for {
      searchPathStmt <- managed(conn.createStatement())
      _ = searchPathStmt.execute("set search_path to test_api, public")
      statement <- managed(conn.createStatement())
      resultSet <- managed(statement.executeQuery("select to_json((f.*)) from get_orders('00000001') as f"))
    } yield {
      val buffer = new ListBuffer[String]
      while (resultSet.next()) {
        buffer += resultSet.getObject(1).toString
      }
      buffer
    }

    result.map(identity).either match {
      case Right(r) =>
        Ok(Json.parse(r.mkString("[", ",", "]")))
      case Left(e) =>
        InternalServerError(views.json.error(e.map(stacktrace): _*))
    }
  }

  def unwrap(t: Throwable): List[Throwable] = t :: Option(t.getCause).map(unwrap).getOrElse(Nil)

  def stacktrace(t: Throwable): String = {
    import java.io.PrintWriter
    val writer = new StringWriter()
    t.printStackTrace(new PrintWriter(writer))
    t.toString + "\n" + writer.toString
  }
}


