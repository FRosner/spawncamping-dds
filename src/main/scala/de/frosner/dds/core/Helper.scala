package de.frosner.dds.core

import java.io._

case class Helper[T](classWithHelp: Class[T]) {

  type Name = String
  type ShortDescription = String
  type LongDescription = String

  val methods = {
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

  def printAllMethods(out: PrintStream) = methods.foreach {
    case (category, methods) => {
      out.println(s"\033[1m${category}\033[0m")
      methods.foreach {
        case (name, help) =>
          out.println("- " + getMethodSignature(name, help) +
            s": ${help.shortDescription}")
      }
      out.println()
    }
  }

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

  def printMethods(methodName: String, out: PrintStream) = {
    val methodsToPrint = methods.flatMap {
      case (category, methods) => methods.filter {
        case (name, help) => name == methodName
      }
    }
    methodsToPrint.sortBy {
      case (name, help) => getMethodSignature(name, help)
    }.foreach {
      methodWithHelp => printLongDescription(methodWithHelp, out)
    }
  }

  private def printLongDescription(methodWithHelp: (String, Help), out: PrintStream) = {
    val (name, help) = methodWithHelp
    out.println(s"\033[1m${getMethodSignature(name, help)}\033[0m")
    out.println(help.longDescription)
    out.println()
  }

}
