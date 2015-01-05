package database

import java.util.concurrent.ForkJoinPool

import akka.actor.{Props, ActorLogging, Actor}

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Random, Success, Failure}

object Loaders {
  object ReloadAll
}

class BlockingPeriodicTask(name: String, f: () => Unit) extends Actor with ActorLogging {

  import Loaders._

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.error(s"Restarting $name: $reason $message")
  }

  override def receive: Receive = {
    case ReloadAll =>
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(new ForkJoinPool())
      log.debug(s"Reloading $name")
      val future = Future {
        f()
        log.debug(s"Reloaded $name")
      }
      future.onComplete {
        case Failure(x) => throw x
        case Success(_) => ()
      }
  }
}

class PeriodicTaskSupervisor extends Actor with ActorLogging {

  override def receive: Receive = {
    case p: Props =>
      import scala.concurrent.ExecutionContext.Implicits.global
      import scala.concurrent.duration._
      // todo maybe not the right one
      val child = context.actorOf(p)
      context.system.scheduler.schedule(Random.nextInt(5).seconds, 5.seconds) {
        child ! Loaders.ReloadAll
      }
  }
}
