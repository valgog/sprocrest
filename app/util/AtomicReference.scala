package util

import java.util.concurrent.atomic

import play.libs.Scala

final class AtomicReference[T <: AnyRef](init: Option[T] = None) { self =>

   val wrapped = new atomic.AtomicReference[T](Scala.orNull(init))

   def get: Option[T] = Option(wrapped.get)
   def getOrElse(default: T): T = get.getOrElse(default)
   def set(v: Option[T]): Unit = wrapped.set(Scala.orNull(v))
   def map[V](f: T => V): Option[V] = get.map(f)
   def flatMap[V](f: T => Option[V]): Option[V] = get.flatMap(f)
   def filter(f: T => Boolean) = get.filter(f)
   def withFilter(p: T => Boolean): WithFilter = new WithFilter(p)

   class WithFilter(p: T => Boolean) {
      def map[B](f: T => B): Option[B] = self filter p map f
      def flatMap[B](f: T => Option[B]): Option[B] = self filter p flatMap f
      def foreach[U](f: T => U): Unit = self filter p foreach f
      def withFilter(q: T => Boolean): WithFilter = new WithFilter(x => p(x) && q(x))
   }
}
