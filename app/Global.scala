import database.StoredProcedures
import play.api.{Application, GlobalSettings}

object Global extends GlobalSettings{
  override def onStart(app: Application): Unit = {
    StoredProcedures.start()
  }
}
