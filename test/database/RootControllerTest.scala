package database

import org.specs2.mutable._
import play.api.libs.json._
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

    "make an call correctly with default argument" in new WithApplication {
      val res = route(FakeRequest(POST, "/call/test_api/get_orders").withJsonBody {
        Json.parse(
          """
            |{
            |  "customer_number": "00001"
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

    "make an call correctly with no arguments" in new WithApplication {
      val res = route(FakeRequest(POST, "/call/test_api/get_customer_count").withJsonBody {
        Json.parse("{}")
      }).get

      status(res) must_== 200
      contentAsJson(res) match {
        case array: JsArray =>
          array.value.size must_== 1
          array.value.head must_== JsNumber(9) // TODO: fix this
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

    "behave as expected looking for non-existing types" in new WithApplication {
      val res = route(FakeRequest(GET, "/types/-1")).get
      status(res) must_== 404
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

    "return a 404 when calling non-existing sproc" in new WithApplication {
      val res = route(FakeRequest(POST, "/call/test_api/get_orders_does_not_exist").withJsonBody {
        Json.parse(
          """
            |{
            |  "customer_number": "00001",
            |  "order_numbers": [ "00001000001" ]
            |}
          """.stripMargin)
      }).get

      status(res) must_== 404
    }

    "return a 400 when calling an overloaded sproc without enough arguments" in new WithApplication {
      val res = route(FakeRequest(POST, "/call/test_api/get_orders").withJsonBody {
        Json.parse(
          """
            |{
            |}
          """.stripMargin)
      }).get

      status(res) must_== 400
    }

    "return a 500 when passing malformed input arguments" in new WithApplication {
      val res = route(FakeRequest(POST, "/call/test_api/get_orders").withJsonBody {
        Json.parse(
          """
            |{
            |  "customer_number": "00001",
            |  "order_numbers": [ 1 ]
            |}
          """.stripMargin)
      }).get

      status(res) must_== 500
    }

    "return a 400 when posting weird json" in new WithApplication {
      val res = route(FakeRequest(POST, "/call/test_api/get_orders").withJsonBody {
        Json.parse(
          """
            |[
            |  "00001", [ 1 ]
            |]
          """.stripMargin)
      }).get

      status(res) must_== 400
    }
  }
}
