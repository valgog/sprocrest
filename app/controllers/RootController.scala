package controllers

import com.gilt.play.json.controllers.JsonController
import play.api.mvc.Action

import scala.language.reflectiveCalls

object RootController extends JsonController {
  override def errorView = views.json.error

  override def https = false

  def get() = Action { implicit request =>
    Ok("hello, world")
  }
}


