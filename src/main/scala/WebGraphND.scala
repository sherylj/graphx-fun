/**
 * Web graph Notre Dame data taken from SNAP project
 * http://snap.stanford.edu/data/web-NotreDame.html
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object WebGraphND {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Webgraph NotreDame dataset")
    val graph = GraphLoader.edgeListFile(sc,"/Users/sherylj/IdeaProjects/graphx-fun/resources/web-NotreDame.txt", minEdgePartitions = 4)


    println("vertices" + graph.vertices.count)
    val cc = graph.connectedComponents.vertices.map(_._2).cache()
    println("connected: " + cc.count)
    println("edges " + graph.edges.count)


  }
}
