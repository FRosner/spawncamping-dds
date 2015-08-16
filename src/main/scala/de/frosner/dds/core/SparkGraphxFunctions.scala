package de.frosner.dds.core

import org.apache.spark.graphx
import org.apache.spark.graphx._

import scala.reflect.ClassTag

object SparkGraphxFunctions {

  private[core] def createShowVertexSample[VD, ED](graph: graphx.Graph[VD, ED],
                                             sampleSize: Int,
                                             vertexFilter: (VertexId, VD) => Boolean): Option[Servable] = {
    val vertexSample = graph.vertices.filter{
      case (id, attr) => vertexFilter(id, attr)
    }.take(sampleSize).map{ case (id, attr) => id }.toSet
    val sampledGraph = graph.subgraph(
      edge => vertexSample.contains(edge.srcId) && vertexSample.contains(edge.dstId),
      (vertexId, vertexAttr) => vertexSample.contains(vertexId)
    )
    ScalaFunctions.createGraph(sampledGraph.vertices.collect.toSeq, sampledGraph.edges.collect.map(edge => (edge.srcId, edge.dstId, edge.attr)))
  }

  private[core] def createShowEdgeSample[VD, ED](graph: graphx.Graph[VD, ED],
                                   sampleSize: Int,
                                   edgeFilter: (Edge[ED]) => Boolean): Option[Servable] = {
    val edgeSample = graph.edges.filter(edgeFilter).take(sampleSize)
    val verticesToKeep = edgeSample.map(_.srcId).toSet ++ edgeSample.map(_.dstId).toSet
    val vertexSample = graph.vertices.filter{ case (id, attr) => verticesToKeep.contains(id) }.collect
    ScalaFunctions.createGraph(vertexSample, edgeSample.map(edge => (edge.srcId, edge.dstId, edge.attr)))
  }

  private[core] def createConnectedComponents[VD: ClassTag, ED: ClassTag](graph: graphx.Graph[VD, ED]): Option[Servable] = {
    val connectedComponents = graph.connectedComponents()
    val vertexCounts = connectedComponents.vertices.map{
      case (id, connectedComponent) => (connectedComponent, 1)
    }.reduceByKey(_ + _)
    val edgeCounts = connectedComponents.edges.map(e => (e.srcId, 1)).join(
      connectedComponents.vertices
    ).map{
      case (id, (count, connectedComponent)) => (connectedComponent, count)
    }.reduceByKey(_ + _)
    val counts = vertexCounts.leftOuterJoin(edgeCounts)
    ScalaFunctions.createTable(
      List("Connected Component", "#Vertices", "#Edges"),
      counts.map{ case (connectedComponent, (numVertices, numEdges)) =>
        List(connectedComponent, numVertices, numEdges.getOrElse(0))
      }.collect
    )
  }


}
