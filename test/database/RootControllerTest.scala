package database

import org.specs2.mutable._
import play.api.libs.json.{JsString, JsObject, Json}
import play.api.test.Helpers._
import play.api.test.{FakeRequest, WithApplication}

class RootControllerTest extends Specification {
  "RootController" should {
    "make the core call correctly" in new WithApplication {
      val res = route(FakeRequest(GET, "/")).get
      status(res) must_== 200
    }

    "make an arbitrary call correctly" in new WithApplication {
      val res = route(FakeRequest(POST, "/call/test_api/get_orders").withJsonBody {
        Json.parse(
          """
            |{
            |  "customer_number": "00000001",
            |  "ignored": 7
            |}
          """.stripMargin)
      }).get

      status(res) must_== 200
    }

    "find some expected types" in new WithApplication {
      val res = route(FakeRequest(GET, "/types")).get
      status(res) must_== 200
      val json = contentAsJson(res).asInstanceOf[JsObject]
      (json \ "16" \ "name").asInstanceOf[JsString].value must_== "bool"
    }
  }
}
