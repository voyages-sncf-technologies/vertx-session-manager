package com.campudus.vertx.sessionmanager

import java.util.UUID

import org.vertx.java.core.AsyncResult
import org.vertx.java.core.AsyncResultHandler
import org.vertx.java.core.Handler
import org.vertx.java.core.Vertx
import org.vertx.java.core.json.JsonArray
import org.vertx.java.core.json.JsonObject

class SharedDataSessionStore(sm: SessionManager, sharedSessionsName: String, sharedSessionTimeoutsName: String) extends SessionManagerSessionStore with VertxScalaHelpers {

  val vertx = sm.getVertx()
  val sharedSessions = vertx.sharedData.getMap[String, String](sharedSessionsName)
  val sharedSessionTimeouts = vertx.sharedData.getMap[String, String](sharedSessionTimeoutsName)

  def clearAllSessions(resultHandler: AsyncResultHandler[Boolean]) = {
    import scala.collection.JavaConversions._
    for ((sessionId, sessionString) <- sharedSessions) {
      sm.destroySession(sessionId, None)
    }
    resultHandler.handle(true)
  }

  def getMatches(data: JsonObject, resultHandler: AsyncResultHandler[JsonArray]) = {
    import scala.collection.JavaConversions._
    val sessionsArray = new JsonArray()
    for ((id, sessionString) <- sharedSessions) {
      val sessionData = new JsonObject(sessionString)
      if (sessionData.copy.mergeIn(data) == sessionData) {
        sessionsArray.addObject(sessionData.putString("sessionId", id))
      }
    }
    resultHandler.handle(sessionsArray)
  }

  def getOpenSessions(resultHandler: AsyncResultHandler[Long]) = resultHandler.handle(sharedSessions.size)

  def getSessionData(sessionId: String, fields: JsonArray, resultHandler: AsyncResultHandler[JsonObject]): Unit = sharedSessions.get(sessionId) match {
    case null =>
      resultHandler.handle(new AsyncResult(new SessionException("SESSION_GONE", "Cannot get data from session '" + sessionId + "'. Session is gone.")))
    case sessionString =>
      // session is still in use, change timeout
      val session = new JsonObject(sessionString)
      val result = new JsonObject
      for (key <- fields.toArray) {
        session.getField(key.toString) match {
          case null => // Unknown field: Do not put into result
          case elem: JsonArray => result.putArray(key.toString, elem)
          case elem: Array[Byte] => result.putBinary(key.toString, elem)
          case elem: java.lang.Boolean => result.putBoolean(key.toString, elem)
          case elem: Number => result.putNumber(key.toString, elem)
          case elem: JsonObject => result.putObject(key.toString, elem)
          case elem: String => result.putString(key.toString, elem)
          case unknownType => // Unknown type: Do not put into result
        }
      }
      resultHandler.handle(new JsonObject().putObject("data", result))
  }

  def putSession(sessionId: String, data: JsonObject, resultHandler: AsyncResultHandler[Boolean]) = {
    sharedSessions.get(sessionId) match {
      case null =>
        resultHandler.handle(new AsyncResult(new SessionException("SESSION_GONE", "Could not find session with id '" + sessionId + "'.")))
      case sessionString =>
        val session = new JsonObject(sessionString)
        session.mergeIn(data)
        sharedSessions.put(sessionId, session.encode)
        resultHandler.handle(true)
    }
  }

  def removeSession(sessionId: String, timerId: Option[Long], resultHandler: AsyncResultHandler[JsonObject]) {
    val actualTimerId = if (timerId.isDefined) {
      if (sharedSessionTimeouts.remove(sessionId, timerId.get.toString)) {
        timerId.get
      } else {
        return
      }
    } else {
      sharedSessionTimeouts.remove(sessionId).toLong
    }
    resultHandler.handle(
      new JsonObject().putNumber("sessionTimer", actualTimerId).
        putObject("session", new JsonObject(sharedSessions.remove(sessionId))))
  }

  def resetTimer(sessionId: String, newTimerId: Long, resultHandler: AsyncResultHandler[Long]) = sharedSessionTimeouts.get(sessionId) match {
    case null =>
      resultHandler.handle(new AsyncResult(new SessionException("UNKNOWN_SESSIONID", "Could not find session with id '" + sessionId + "'.")))
    case timerId =>
      if (sharedSessionTimeouts.replace(sessionId, timerId, newTimerId.toString)) {
        resultHandler.handle(timerId.toLong)
      } else {
        resultHandler.handle(new AsyncResult(new SessionException("TIMER_EXPIRED", "Could not reset timer for session with id '" + sessionId + "'.")))
      }
  }

  def startSession(resultHandler: AsyncResultHandler[String]) = {
    val sessionId = UUID.randomUUID.toString
    sharedSessions.putIfAbsent(sessionId, "{}") match {
      case null =>
        sharedSessionTimeouts.put(sessionId, sm.createTimer(sessionId).toString)
        // There is no session with this uuid -> return it
        resultHandler.handle(sessionId)
      case anotherSessionId =>
        // There was a session with this uuid -> create a new one
        startSession(resultHandler)
    }
  }
}