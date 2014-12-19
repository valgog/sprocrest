package database

import org.specs2.mutable._
import play.api.libs.json.{JsArray, JsString, JsObject, Json}
import play.api.test.Helpers._
import play.api.test.{FakeRequest, WithApplication}

class RootControllerTest extends Specification {
  "RootController" should {
    "make an arbitrary call correctly" in new WithApplication {
      val res = route(FakeRequest(POST, "/call/test_api/get_orders").withJsonBody {
        Json.parse(
          """
            |{
            |  "customer_number": "00001",
            |  "ignored": 7
            |}
          """.stripMargin)
      }).get

      status(res) must_== 200
      contentAsJson(res) match {
        case array: JsArray =>
          array.value.size must_== 5
          (array.value.head \ "order_number").asInstanceOf[JsString].value must_== "00001000001"
        case other => sys.error(s"Unexpected result type: $other")
      }
    }

    "make an arbitrary call correctly with an array" in new WithApplication {
      val res = route(FakeRequest(POST, "/call/test_api/get_orders").withJsonBody {
        Json.parse(
          """
            |{
            |  "customer_number": "00001",
            |  "order_numbers": [ "00001000001" ]
            |}
          """.stripMargin)
      }).get

      status(res) must_== 200
      contentAsJson(res) match {
        case array: JsArray =>
          array.value.size must_== 1
          (array.value.head \ "order_number").asInstanceOf[JsString].value must_== "00001000001"
        case other => sys.error(s"Unexpected result type: $other")
      }
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

    "arbitrary call with ignored works" in new WithApplication {

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
