package de.frosner.dds.util

import org.scalatest.{Matchers, FlatSpec}

class BinaryResourceTest extends FlatSpec with Matchers {

  "A binary resource" should "read text files correctly" in {
    BinaryResource.read("/test.txt") shouldBe "This is a test!".toCharArray.map(_.toByte)
  }

}
