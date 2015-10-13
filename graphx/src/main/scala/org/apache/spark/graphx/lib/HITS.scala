package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.graphx._

/*
 * Hyperlink-Induced Topic Search algorithm implementation.
 */
object HITS extends Logging {

  /**
   * Run HITS for a fixed number of iterations returning a graph
   * with vertex attributes containing the authority and hub scores.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute HITS
   * @param numIter the number of iterations of HITS to run
   *
   * @return the graph containing with each vertex containing a tuple of
   * the authority score and the hub score.
   */
  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), Unit] =
  {
    // Initialize authority and hub scores, the first value is the authority
    // score and the second is the hub score. Initialize the edge message to unit
    var rankGraph: Graph[(Double, Double), Unit] = graph
      .mapVertices((vid, vdata) => (1D, 1D))
      .mapEdges(e => ())

    var iteration = 0
    while (iteration < numIter) {
      rankGraph.cache()
      // update authority scores
      val authority_updates = rankGraph
        .aggregateMessages[Double](ctx => ctx.sendToDst(ctx.srcAttr._2), _ + _, TripletFields.Src)
      rankGraph = rankGraph.outerJoinVertices(authority_updates) {
        (vid, scores, authority_update) => (authority_update.getOrElse(0), scores._2)
      }

      // update hub scores
      val hub_updates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr._1), _ + _, TripletFields.Dst
      )
      rankGraph = rankGraph.outerJoinVertices(hub_updates) {
        (vid, scores, hub_update) => (scores._1, hub_update.getOrElse(0))
      }

      // normalize scores
      val authority_total = rankGraph.vertices
        .map { case (vid, vdata) => vdata._1 }
        .sum()
      val hub_total = rankGraph.vertices
        .map { case (vid, vdata) => vdata._2 }
        .sum()
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
