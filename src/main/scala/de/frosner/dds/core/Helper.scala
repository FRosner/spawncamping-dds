package de.frosner.dds.core

import java.io._

/**
 * Utility class to provide interactive help for methods of a given class. It scans the class method definitions for
 * the ones that contain a [[Help]] annotation. Use it to print a list of methods with help available or to print
 * a longer description for individual methods.
 *
 * @param classWithHelp to scan for [[Help]] annotations
 * @tparam T type of the class
 */
case class Helper[T](classWithHelp: Class[T]) {

  import Helper.NEWLINE
  type Name = String
  type ShortDescription = String
  type LongDescription = String

  private[core] val methods = {
    val methodsThatOfferHelp = classWithHelp.getMethods.filter(method => method.getAnnotations.exists(
      annotation => annotation.isInstanceOf[Help]
    ))
    val methodsAndHelp = methodsThatOfferHelp.map(method => {
      val helpAnnotation = method.getAnnotations.find(annotation =>
        annotation.isInstanceOf[Help]
      ).get.asInstanceOf[Help]
      (method.getName, helpAnnotation)
    })
    methodsAndHelp.groupBy {
      case (name, help) => help.category()
    }.toList.sortBy {
      case (category, methods) => category.toLowerCase
    }.map {
      case (category, methods) => (category, methods.sortBy {
        case (name, help) => name
      })
    }
  }

  /**
   * Prints all methods that offer help (have the [[Help]] annotation) to the given [[PrintStream]].
   *
   * @param out to print method overview to
   */
  def printAllMethods(out: PrintStream) = out.println(
    methods.map {
      case (category, methods) => {
        s"\033[1m${category}\033[0m${NEWLINE}" + methods.map {
          case (name, help) => "- " + getMethodSignature(name, help) + s": ${help.shortDescription}"
        }.mkString(NEWLINE)
      }
    }.mkString(NEWLINE+NEWLINE)
  )

  private def getMethodSignature(name: String, help: Help) = {
    s"$name(${help.parameters})" +
      (if (help.parameters2() != "") { "(" + help.parameters2 + ")" } else "") +
      (if (help.parameters3() != "") { "(" + help.parameters3 + ")" } else "") +
      (if (help.parameters4() != "") { "(" + help.parameters4 + ")" } else "") +
      (if (help.parameters5() != "") { "(" + help.parameters5 + ")" } else "") +
      (if (help.parameters6() != "") { "(" + help.parameters6 + ")" } else "") +
      (if (help.parameters7() != "") { "(" + help.parameters7 + ")" } else "") +
      (if (help.parameters8() != "") { "(" + help.parameters8 + ")" } else "") +
      (if (help.parameters9() != "") { "(" + help.parameters9 + ")" } else "")
  }

  /**
   * Prints the long description of all methods having the given name to the specified [[PrintStream]].
   *
   * @param methodName to print help for
   * @param out to print help to
   */
  def printMethods(methodName: String, out: PrintStream) = {
    val methodsToPrint = methods.flatMap {
      case (category, methods) => methods.filter {
        case (name, help) => name == methodName
      }
    }
    out.println(
      methodsToPrint.sortBy {
        case (name, help) => getMethodSignature(name, help)
      }.map {
        methodWithHelp => getLongDescriptionPrintable(methodWithHelp, out)
      }.mkString(NEWLINE+NEWLINE)
    )
  }

  private def getLongDescriptionPrintable(methodWithHelp: (String, Help), out: PrintStream) = {
    val (name, help) = methodWithHelp
    s"\033[1m${getMethodSignature(name, help)}\033[0m${NEWLINE}" + help.longDescription
  }

}

object Helper {
  val NEWLINE = System.getProperty("line.separator")
}
