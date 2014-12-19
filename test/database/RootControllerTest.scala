package database

import org.specs2.mutable._
import play.api.libs.json.{JsArray, JsString, JsObject, Json}
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

    "find a type by its id" in new WithApplication {
      val res = route(FakeRequest(GET, "/types/16")).get
      status(res) must_== 200
      val json = contentAsJson(res).asInstanceOf[JsObject]
      (json \ "name").asInstanceOf[JsString].value must_== "bool"
    }

    "find some expected procs" in new WithApplication {
      val res = route(FakeRequest(GET, "/procs")).get
      status(res) must_== 200
      val json = contentAsJson(res).asInstanceOf[JsObject]
      ((json \ "(test_api,get_orders)").asInstanceOf[JsArray](0) \ "name").asInstanceOf[JsString].value must_== "get_orders"
    }

    "find some expected procs by namespace" in new WithApplication {
      val res = route(FakeRequest(GET, "/procs/test_api")).get
      status(res) must_== 200
      val json = contentAsJson(res).asInstanceOf[JsObject]
      ((json \ "(test_api,get_orders)").asInstanceOf[JsArray](0) \ "name").asInstanceOf[JsString].value must_== "get_orders"
    }

    "find some exact procs" in new WithApplication {
      val res = route(FakeRequest(GET, "/procs/test_api/get_orders")).get
      status(res) must_== 200
      val json = contentAsJson(res).asInstanceOf[JsObject]
      ((json \ "(test_api,get_orders)").asInstanceOf[JsArray](0) \ "name").asInstanceOf[JsString].value must_== "get_orders"
    }

    "find an argument in the test_api namespace" in new WithApplication {
      val res = route(FakeRequest(GET, "/arguments")).get
      status(res) must_== 200
      val json = contentAsJson(res).asInstanceOf[JsArray]
      json.value.exists {
        case jsObject: JsObject => (jsObject \ "namespace").asInstanceOf[JsString].value == "test_api"
        case _ => false
      }
    }
  }
}
