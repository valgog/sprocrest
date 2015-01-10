import database.{DB, StoredProcedures}
import play.api.{Application, GlobalSettings}

object Global extends GlobalSettings {

  override def onStart(app: Application): Unit = {
    DB.onStart(app)
    StoredProcedures.start()
  }

  override def onStop(app: Application): Unit = {
    DB.onStop(app)
  }
}
