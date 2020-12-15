/*
Author: Md Arafat Hossain Khan
Department: Mathematics
 */

import org.apache.spark.sql.functions._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.graphframes._
import org.apache.spark.{SparkConf, SparkContext}

object graphAssignmentArafat {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: graphAssignmentArafat InputDir OutputDir")
    }
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark graphAssignmentArafat"))
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val Q0 = "This project was created by\n"+
      "Md Arafat Hossain Khan\n"+
      "PhD Candidate\n"+
      "Department of Mathematics\nUniversity of Texas at Dallas"

    ////////////////// Code Start ///////////////////////////

    // read in csv file and split each document into words
    val airport = sqlContext.read.option("header","true").option("inferSchema","true").csv(args(0))

    /////////// -------- Generic Code Start --------- ///////////////////

    val graphTable = airport.groupBy("ORIGIN","DEST").count().orderBy(desc("count"))
      .toDF("Origin","Dest","count")
    val nodesOriginDest = graphTable.select("ORIGIN").collect.map(row=>row.getString(0)) ++
      graphTable.select("DEST").collect.map(row=>row.getString(0))
    val nodes = nodesOriginDest.distinct
    val n = nodes.length
    var V = Array[(Long, (String, Int))]()
    var V2 = Array[(Long, String, Int)]()
    for( i <- 1 to n){
      V = V :+ (i.toLong,(nodes(i-1),1))
      V2 = V2 :+ (i.toLong,nodes(i-1),1)
    }
    var currentRow = graphTable.collect()(0).toSeq.toArray.map(_.toString)
    var E = Array(
      Edge(V(V.indexWhere(_._2._1 == currentRow(0)))._1,
        V(V.indexWhere(_._2._1 == currentRow(1)))._1,currentRow(2).toInt)
    )
    var m = graphTable.count()
    for( i <- 1 to (m-1).toInt){
      currentRow = graphTable.collect()(i).toSeq.toArray.map(_.toString)
      E = E :+ Edge(V(V.indexWhere(_._2._1 == currentRow(0)))._1,
        V(V.indexWhere(_._2._1 == currentRow(1)))._1,currentRow(2).toInt)
    }
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(V)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(E)
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    // 1. Find the total number of airports (vertices) and the total flights connecting them.
    var Q1 = "1. Find the total number of airports (vertices) and the total flights connecting them." +
      "\n Total number of airports :: " + graph.vertices.count + "\n Total number of flights :: " +
      graph.edges.toDF.select(col("attr")).rdd.map(_(0).asInstanceOf[Int]).reduce(_+_)
    // 2. Find the in-degree and out-degree of each airport.
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    val outDegrees: VertexRDD[Int] = graph.outDegrees
    var inDegreeString = ""
    inDegrees // computes in Degrees
      .join(vertexRDD)
      .sortBy(_._2._1, ascending=false)
      .collect()
      .foreach(x => inDegreeString = inDegreeString + (x._2._2._1, x._2._1) + ",")
    inDegreeString = inDegreeString.dropRight(1)
    var outDegreeString = ""
    outDegrees // computes in Degrees
      .join(vertexRDD)
      .sortBy(_._2._1, ascending=false)
      .collect()
      .foreach(x => outDegreeString = outDegreeString + (x._2._2._1, x._2._1) + ",")
    outDegreeString = outDegreeString.dropRight(1)
    var Q2 = "\n\n2. Find the in-degree and out-degree of each airport.\n"
    Q2 += "in-degree :: " + outDegreeString
    Q2 += "\nout-degree :: " + outDegreeString
    // 3. Find the top 10 airports with the highest in-degree and out-degree values.
    inDegreeString = ""
    inDegrees // computes in Degrees
      .join(vertexRDD)
      .sortBy(_._2._1, ascending=false)
      .take(10)
      .foreach(x => inDegreeString = inDegreeString + (x._2._2._1, x._2._1) + ",")
    inDegreeString = inDegreeString.dropRight(1)
    //graph.vertices.collect()
    outDegreeString = ""
    outDegrees // computes in Degrees
      .join(vertexRDD)
      .sortBy(_._2._1, ascending=false)
      .take(10)
      .foreach(x => outDegreeString = outDegreeString + (x._2._2._1, x._2._1) + ",")
    outDegreeString = outDegreeString.dropRight(1)
    var Q3 = "\n\n3. Find the top 10 airports with the highest in-degree and out-degree values.\n"
    Q3 += "in-degree :: " + outDegreeString
    Q3 += "\nout-degree :: " + outDegreeString
    // 4. Find out how many triangles can be formed around Dallas Fort Worth International (DFW) airport.
    var triangleAirport = "DFW"
    var vCode = graph.vertices.filter(x=>x._2._1==triangleAirport).collect()(0)._1
    var Q4 = "\n\n4. Find out how many triangles can be formed around Dallas Fort Worth International (DFW) airport.\n"
    Q4 += "Total number of triangles formed around " + triangleAirport + " :: "
    + graph.triangleCount().vertices.filter(x=>x._1==vCode).collect()(0)._2
    // 5. Find out the top 10 airports with the highest page ranks.
    var top10 = ""
    val ranks = graph.pageRank(0.1).vertices
    val topVertices = ranks.sortBy(_._2,false).take(10).foreach(x=> top10 = top10 + "(" +
      graph.vertices.collect()(graph.vertices.collect().indexWhere(_._1== x._1))._2._1 + "," + x._2 + "),")
    var Q5 = "\n\n5. Find out the top 10 airports with the highest page ranks.\n"
    Q5 += "Top 10 airports with the highest page ranks  :: " + top10.dropRight(1)

    // 6. Find triplets A, B, and C such that, at least one flight from A to B, and from B to C, but no flight from A to C.
    val v = sqlContext.createDataFrame(V2).toDF("id", "name", "age")
    val e = sqlContext.createDataFrame(E).toDF("src", "dst", "relationship")

    val g = GraphFrame(v, e)
    val motifs = g.find("(A)-[]->(B); (B)-[]->(C); !(A)-[]->(C)")
    var ABCList = motifs.select("A.name", "B.name", "C.name").toDF("A", "B", "C")
    var Q6 = "\n\n6. Triplets [A, B, C] such that, at least one flight from A to B, and from B to C, but no flight from A to C.\n"
    var resultABC = ABCList.rdd.map(_.toString())
    Q6 += resultABC.collect().deep.mkString(",")
    // -------------- Final Answer ----------------------- //



    var Answer = Q0+Q1+Q2+Q3+Q4+Q5

    ////////// --------- Generic Code End --------- ///////////////////

    // Prepare output string
    var answerRDD = sc.parallelize(Seq(Answer))

    ////////////////// Code End ////////////////////////////
    answerRDD.map(r => r +"\n").saveAsTextFile(args(1))
  }
}
