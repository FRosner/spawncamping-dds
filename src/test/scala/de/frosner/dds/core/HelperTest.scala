package de.frosner.dds.core

import java.io.{ByteArrayOutputStream, PrintStream}

import org.scalatest.{FlatSpec, Matchers}
import Helper.NEWLINE

class HelperTest extends FlatSpec with Matchers {
  
  class TestClass {
    def noHelp = ???

    @Help(
      category = "a",
      shortDescription = "short help",
      longDescription = "long help"
    )
    def help = ???

    @Help(
      category = "a",
      shortDescription = "short help",
      longDescription = "long help"
    )
    def xhelp = ???

    @Help(
      category = "bbb",
      shortDescription = "sph",
      longDescription = "long parameter help",
      parameters = "i: Int",
      parameters2 = "s: String"
    )
    def helpWithParameters(i: Int) = ???
  }

  "A helper" should "offer only help for methods with the correct annotation" in {
    val testClass = new TestClass()
    val helper = Helper(testClass.getClass)
    helper.methods.toMap.keySet shouldBe Set("a", "bbb")

    val (aMethodName, aMethodHelp) = helper.methods(0)._2(0)
    aMethodName shouldBe "help"
    aMethodHelp.category shouldBe "a"
    aMethodHelp.shortDescription shouldBe "short help"
    aMethodHelp.longDescription shouldBe "long help"

    val (bMethodName, bMethodHelp) = helper.methods(1)._2(0)
    bMethodName shouldBe "helpWithParameters"
    bMethodHelp.category shouldBe "bbb"
    bMethodHelp.shortDescription shouldBe "sph"
    bMethodHelp.longDescription shouldBe "long parameter help"
    bMethodHelp.parameters shouldBe "i: Int"
    bMethodHelp.parameters2 shouldBe "s: String"
  }

  it should "print only the short description in the method listing" in {
    val result = new ByteArrayOutputStream()
    val out = new PrintStream(result)
    val helper = Helper(new TestClass().getClass)
    helper.printAllMethods(out)
    result.toString.split(NEWLINE, -1) shouldBe Array(
        s"\033[1ma\033[0m",
        "- help(): short help",
        "- xhelp(): short help",
        "",
        s"\033[1mbbb\033[0m",
        "- helpWithParameters(i: Int)(s: String): sph",
        ""
      )
  }

  it should "print the long description if a method help is requested" in {
    val result = new ByteArrayOutputStream()
    val out = new PrintStream(result)
    val helper = Helper(new TestClass().getClass)
    helper.printMethods("help", out)
    result.toString.split(NEWLINE, -1) shouldBe Array(
        s"\033[1mhelp()\033[0m",
        "long help",
        ""
      )
  }

  it should "print help for multiple methods if there are multiple methods with the same name but different parameters" in {
    class TestClass2 {
      @Help(
        category = "category",
        shortDescription = "without parameters",
        longDescription = "method without parameters"
      )
      def method = ???
      @Help(
        category = "category",
        shortDescription = "with parameters",
        longDescription = "method with one parameter",
        parameters = "s: String"
      )
      def method(s: String) = ???
    }

    val result = new ByteArrayOutputStream()
    val out = new PrintStream(result)
    val helper = Helper(new TestClass2().getClass)
    helper.printMethods("method", out)
    result.toString.split(NEWLINE, -1) shouldBe Array(
      s"\033[1mmethod()\033[0m",
      "method without parameters",
      "",
      s"\033[1mmethod(s: String)\033[0m",
      "method with one parameter",
      ""
    )
  }


}
