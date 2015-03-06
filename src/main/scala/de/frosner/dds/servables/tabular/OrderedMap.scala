package de.frosner.dds.servables.tabular

/**
 * Map implementation that returns an iterator respecting the sequence of the input values. It is used to ensure the
 * ordering of the JSON object keys sent to the front-end, so that D3 displays them in the desired order.
 *
 * @param entries of the map
 * @tparam A key type
 * @tparam B value type
 */
case class OrderedMap[A, B](entries: Seq[(A, B)]) extends Map[A, B] {
  lazy val entriesMap = entries.toMap

  override def +[B1 >: B](kv: (A, B1)): Map[A, B1] = entriesMap.+(kv)

  override def get(key: A): Option[B] = entriesMap.get(key)

  override def iterator: Iterator[(A, B)] = entries.iterator

  override def -(key: A): Map[A, B] = entriesMap.-(key)
}
