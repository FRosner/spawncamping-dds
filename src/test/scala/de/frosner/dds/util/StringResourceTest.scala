package de.frosner.dds.util

import org.scalatest.{FlatSpec, Matchers}

class StringResourceTest extends FlatSpec with Matchers {

  "A string resource" should "return the contents of a resource as a string" in {
    StringResource.read("/test.txt") shouldBe "This is a test!"
  }

}
