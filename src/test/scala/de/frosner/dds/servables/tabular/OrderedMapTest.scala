package de.frosner.dds.servables.tabular

import org.scalatest.{FlatSpec, Matchers}

class OrderedMapTest extends FlatSpec with Matchers {

   "An ordered map" should "add entries correctly but the resulting map is not ordered anymore" in {
     val map = OrderedMap(List(("a", 1), ("b", 2), ("c", 3)))
     val extendedMap = map + (("d", 4))
     extendedMap.keySet shouldBe Set("a", "b", "c", "d")
     extendedMap("a") shouldBe 1
     extendedMap("b") shouldBe 2
     extendedMap("c") shouldBe 3
     extendedMap("d") shouldBe 4
   }

  it should "allow to retrieve the entries as they were passed in" in {
    val map = OrderedMap(List(("a", 1), ("b", 2)))
    map("a") shouldBe 1
    map("b") shouldBe 2
  }

  it should "remove entries correctly but the resulting map is not ordered anymore" in {
    val map = OrderedMap(List(("a", 1), ("b", 2), ("c", 3)))
    val extendedMap = map - "a"
    extendedMap.keySet shouldBe Set("b", "c")
    extendedMap("b") shouldBe 2
    extendedMap("c") shouldBe 3
  }

  it should "return a ordered iterator" in {
    val orderedValues = List(("first", 1), ("second", 2), ("third", 3))
    val map = OrderedMap(orderedValues)
    map.iterator.toList shouldBe orderedValues
  }

 }
