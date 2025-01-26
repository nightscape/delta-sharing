package io.delta.standalone.internal

package object util {
  def newManualClock: ManualClock = new ManualClock()
}
