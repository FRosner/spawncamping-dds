package de.frosner.dds.core

import java.io.{ByteArrayOutputStream, PrintStream}

import org.scalatest.{FlatSpec, Matchers}

class HelperTest extends FlatSpec with Matchers {
  
  class TestClass {
    def noHelp = ???

    @Help(shortDescription = "short help", longDescription = "long help")
    def help = ???

    @Help(shortDescription = "sph", longDescription = "long parameter help", parameters = "i: Int")
    def helpWithParameters(i: Int) = ???
  }

  "A helper" should "offer only help for methods with the correct annotation" in {
    val testClass = new TestClass()
    val helper = Helper(testClass.getClass)
    helper.methods should contain only (
      ("help", "", "short help", "long help"),
      ("helpWithParameters", "i: Int", "sph", "long parameter help")
    )
  }

  it should "print only the short description in the method listing" in {
    val result = new ByteArrayOutputStream()
    val out = new PrintStream(result)
    val helper = Helper(new TestClass().getClass)
    helper.printMethods(out)
    result.toString.split("\n") should contain only ("help(): short help", "helpWithParameters(i: Int): sph")
  }

}
