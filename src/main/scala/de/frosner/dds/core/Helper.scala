package de.frosner.dds.core

import java.io._

case class Helper[T](classWithHelp: Class[T]) {

  type Name = String
  type ShortDescription = String
  type LongDescription = String

  val methods = {
    val methodsWithHelp = classWithHelp.getMethods.filter(method => method.getAnnotations.exists(
      annotation => annotation.isInstanceOf[Help]
    ))
    methodsWithHelp.map(method => {
      val helpAnnotation = method.getAnnotations.find(annotation =>
        annotation.isInstanceOf[Help]
      ).get.asInstanceOf[Help]
      (method.getName, helpAnnotation.parameters(), helpAnnotation.shortDescription(), helpAnnotation.longDescription())
    })
  }

  def printMethods(out: PrintStream) = methods.foreach {
    case (name, parameters, shortDescription, longDescription) =>
      out.println(s"$name($parameters): $shortDescription")
  }

}
