package de.frosner.dds.util

import java.io.FileInputStream

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object BinaryResource {

  def read(resourceLocation: String) = {
    val in = this.getClass.getResourceAsStream(resourceLocation)
    var byte: Int = in.read()
    var buffer: ArrayBuffer[Byte] = new ArrayBuffer()
    while (byte != -1) {
      buffer += byte.toByte
      byte = in.read()
    }
    buffer.toArray
  }

}
