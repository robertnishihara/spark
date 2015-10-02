package org.apache.spark.graphx.lib

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

// unnecessary?
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.HITS
import org.apache.spark.rdd.RDD


class HITSSuite extends SparkFunSuite with LocalSparkContext {

  def compareScores(a: VertexRDD[(Double, Double)], b: VertexRDD[(Double, Double)]): Double = {
    a.leftJoin(b) {
      case (id, a, bOpt) =>
        (a._1 - bOpt.getOrElse((0.0, 0.0))._1) * (a._1 - bOpt.getOrElse((0.0, 0.0))._1) +
        (a._2 - bOpt.getOrElse((0.0, 0.0))._2) * (a._2 - bOpt.getOrElse((0.0, 0.0))._2)
    }
      .map { case (id, error) => error }.sum()
  }

  test("Chain HITS") {
    withSpark { sc =>
      val vertices: RDD[(VertexId, Unit)] = sc.parallelize(
        Array((1L, ()), (2L, ()), (3L, ()), (4L, ()), (5L, ()))
      )
      val edges: RDD[Edge[Unit]] = sc.parallelize(
        Array(Edge(1L, 2L, ()), Edge(2L, 3L, ()), Edge(3L, 4L, ()), Edge(4L, 5L, ()))
      )
      val graph = Graph(vertices, edges)


      val rankGraph0 = HITS.run(graph, 0)
      val verticesGroundTruth0: RDD[(VertexId, (Double, Double))] = sc.parallelize(
        Array((1L, (1.0, 1.0)), (2L, (1.0, 1.0)), (3L, (1.0, 1.0)),
          (4L, (1.0, 1.0)), (5L, (1.0, 1.0)))
      )
      assert(compareScores(rankGraph0.vertices, VertexRDD(verticesGroundTruth0)) < 1e-6)

      val rankGraph1 = HITS.run(graph, 1)
      val verticesGroundTruth1: RDD[(VertexId, (Double, Double))] = sc.parallelize(
        Array((1L, (0.0, 0.0)), (2L, (1.0 / 4.0, 0.0)), (3L, (1.0 / 4.0, 1.0 / 3.0)),
          (4L, (1.0 / 4.0, 1.0 / 3.0)), (5L, (1.0 / 4.0, 1.0 / 3.0)))
      )
      assert(compareScores(rankGraph1.vertices, VertexRDD(verticesGroundTruth1)) < 1e-6)

      val rankGraph10 = HITS.run(graph, 10)
      val verticesGroundTruth10: RDD[(VertexId, (Double, Double))] = sc.parallelize(
        Array((1L, (0.0, 0.0)), (2L, (0.0, 0.0)), (3L, (0.0, 0.0)),
          (4L, (0.0, 0.0)), (5L, (0.0, 0.0)))
      )
      assert(compareScores(rankGraph10.vertices, VertexRDD(verticesGroundTruth10)) < 1e-6)
    }
  }
}
