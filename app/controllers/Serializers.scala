package controllers

import database.CustomTypes.{Name, Namespace, OID}
import database.{Database, StoredProcedure, DbType}
import play.api.libs.json._

object Serializers {

  implicit val oidWriter = new Writes[OID] {
    override def writes(o: OID) = JsString(o.toString)
  }

  implicit val mapOidDbType = new Writes[Map[OID, DbType]] {
    override def writes(o: Map[OID, DbType]): JsValue = {
      JsObject(o.map(kv => kv._1.toString -> Json.toJson(kv._2)).toSeq)
    }
  }

  implicit val writeNameToSprocs = new Writes[Map[(Namespace,Name), Seq[StoredProcedure]]] {
    override def writes(sprocs: Map[(Namespace, Name), Seq[StoredProcedure]]): JsValue = {
      JsObject(sprocs.map(kv => kv._1.toString -> Json.toJson(kv._2)).toSeq)
    }
  }

  implicit val writeDbToTypesMap = new Writes[Map[Database,Map[(Namespace,Name), Seq[StoredProcedure]]]] {
    override def writes(o: Map[Database, Map[(Namespace, Name), Seq[StoredProcedure]]]): JsValue = {
      JsObject {
        o.map {
          case (database, sprocs) =>
            database.name.toString -> Json.toJson(sprocs)
        }.toSeq
      }
    }
  }

  // Map[database.Database,Map[database.CustomTypes.OID,database.DbType]]
  implicit val writeDbtoDbTypes = new Writes[Map[Database,Map[OID,DbType]]] {
    override def writes(o: Map[Database, Map[OID, DbType]]): JsValue = {
      JsObject {
        o.map {
          case (db, map) => db.name -> Json.toJson(map)
        }.toSeq
      }
    }
  }
}
