package database

import play.api.libs.json.JsValue

object Invoker {
  def invoke(namespace: String, proc: String, arguments: JsValue): JsValue = {
    null
  }
}
