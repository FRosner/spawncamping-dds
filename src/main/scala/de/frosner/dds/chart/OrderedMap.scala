package de.frosner.dds.chart

case class OrderedMap[A, B](entries: Seq[(A, B)]) extends Map[A, B] {
  lazy val entriesMap = entries.toMap

  override def +[B1 >: B](kv: (A, B1)): Map[A, B1] = entriesMap.+(kv)

  override def get(key: A): Option[B] = entriesMap.get(key)

  override def iterator: Iterator[(A, B)] = entries.iterator

  override def -(key: A): Map[A, B] = entriesMap.-(key)
}
