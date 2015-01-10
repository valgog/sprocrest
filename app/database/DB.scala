package database

import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.pool.{PoolConfiguration, ConnectionPool}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import database.CustomTypes._
import play.api.Application
import util.AtomicReference

object DB {
  private val pools = new AtomicReference[Map[DatabaseName, ConnectionPool[PostgreSQLConnection]]]

  def pool(name: DatabaseName): Option[ConnectionPool[PostgreSQLConnection]] = pools.get.flatMap(_.get(name))

  def databaseNames = pools.map(_.keys).getOrElse(Nil)

  def databases: Iterable[(DatabaseName, ConnectionPool[PostgreSQLConnection])] = pools.getOrElse(Map.empty)

  def onStart(app: Application): Unit = {
    import scala.collection.JavaConverters._
    val dbConfigs: Map[String, Configuration] =
      app.configuration.getConfigList("db.configs").map(_.asScala).getOrElse(Nil).map { cfg =>
        cfg.getString("name").getOrElse(sys.error("name is required")) -> Configuration(
          username = cfg.getString("username").getOrElse(sys.error("username required")),
          host = cfg.getString("host").getOrElse("localhost"),
          password = cfg.getString("password"),
          port = cfg.getInt("port").getOrElse(5432),
          database = cfg.getString("database")
        )
      }.toMap

    val pools: Map[DatabaseName, ConnectionPool[PostgreSQLConnection]] = dbConfigs.map { case (name, cfg) =>
      val factory = new PostgreSQLConnectionFactory(cfg)
      (name: DatabaseName) -> new ConnectionPool(factory, PoolConfiguration.Default)
    }
    this.pools.set(Some(pools))
  }

  def onStop(app: Application): Unit = {
    this.pools.get.foreach { case map =>
      map.foreach { case (name, pool) =>
        pool.close
      }
    }
  }
}
