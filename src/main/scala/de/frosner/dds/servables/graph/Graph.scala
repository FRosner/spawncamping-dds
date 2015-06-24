package de.frosner.dds.servables.graph

import de.frosner.dds.core.Servable
import spray.json._

/**
 * [[Servable]] representing a D3 graph for the force layout. Vertices are represented as a sequence of Strings
 * that are going to be used as a label. The edges are represented by pairs of integers (source, target) such
 * that the source and target corresponds to the index in the vertex sequence.
 *
 * @param vertices where each vertex has a label
 * @param edges where each edge is a pair of source and target represented by the index in the vertex sequence
 */
case class Graph(vertices: Seq[String],
                 edges: Iterable[(Int, Int, String)],
                 title: String = Servable.DEFAULT_TITLE) extends Servable {

  val servableType = "graph"

  override protected def contentAsJson: JsValue = {
    JsObject(
      ("vertices", JsArray(vertices.map(v => JsObject(("label", JsString(v)))).toVector)),
      ("edges", JsArray(edges.map{ case (src, target, label) => JsObject(
        ("source", JsNumber(src)),
        ("target", JsNumber(target)),
        ("label", JsString(label))
      ) }.toVector))
    )
  }

}
