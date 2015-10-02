package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.graphx._

object HITS extends Logging {

  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), Unit] =
  {

    // Initialize authority and hub scores, the first value is the authority
    // score and the second is the hub score. Initialize the edge message to unit
    var rankGraph: Graph[(Double, Double), Unit] = graph
      .mapVertices((vid, vdata) => (1D, 1D))
      .mapTriplets(e => ())

    var iteration = 0
    while (iteration < numIter) {
      rankGraph.cache()
      // update authority scores
      val authority_updates = rankGraph
        .aggregateMessages[Double](ctx => ctx.sendToDst(ctx.srcAttr._2), _ + _)
      rankGraph = rankGraph.outerJoinVertices(authority_updates) {
        (vid, scores, authority_update) => (authority_update.getOrElse(0), scores._2)
      }

      // update hub scores
      val hub_updates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr._1), _ + _
      )
      rankGraph = rankGraph.outerJoinVertices(hub_updates) {
        (vid, scores, hub_update) => (scores._1, hub_update.getOrElse(0))
      }

      // normalize scores
      val authority_total = rankGraph.vertices
        .map { case (vid, vdata) => vdata._1 }
        .reduce((a, b) => a + b)
      val hub_total = rankGraph.vertices
        .map { case (vid, vdata) => vdata._2 }
        .reduce((a, b) => a + b)
      if (authority_total != 0) {
        rankGraph = rankGraph.mapVertices((vid, vdata) => (vdata._1 / authority_total, vdata._2))
      }
      if (hub_total != 0) {
        rankGraph = rankGraph.mapVertices((vid, vdata) => (vdata._1, vdata._2 / hub_total))
      }

      iteration += 1
    }
    rankGraph
  }
}

/*
test script...

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.HITS
import org.apache.spark.rdd.RDD

val vertices: RDD[(VertexId, Unit)] = sc.parallelize(Array((1L, ()), (2L, ()), (3L, ()), (4L, ()), (5L, ()), (6L, ())))
val edges: RDD[Edge[Unit]] = sc.parallelize(Array[Edge[Unit]]())
val edges: RDD[Edge[Unit]] = sc.parallelize(Array(Edge(1L, 2L, ()), Edge(2L, 3L, ())))
val graph = Graph(vertices, edges)


val users: RDD[(VertexId, (String, String))] =
  sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
val relationships: RDD[Edge[String]] =
  sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
val defaultUser = ("John Doe", "Missing")
val graph = Graph(users, relationships, defaultUser)

HITS.run(graph, 10).vertices.collect()
*/
