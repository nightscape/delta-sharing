package io.delta.sharing.server

import org.apache.hadoop.fs.RemoteIterator

object Implicits {
  implicit class RichRemoteIterator[T](iter: RemoteIterator[T]) {
    def asScala: Iterator[T] = new Iterator[T] {
      def hasNext: Boolean = iter.hasNext
      def next(): T = iter.next()
    }
  }

}
