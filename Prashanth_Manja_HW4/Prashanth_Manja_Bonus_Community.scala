import org.apache.spark.graphx.Edge
import org.apache.spark.graphx._
import scala.collection.mutable.ListBuffer
import ml.sparkling.graph.operators.OperatorsDSL._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.SparkSession
import ml.sparkling.graph.operators.OperatorsDSL._
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN


object Prashanth_Manja_Bonus_Communities {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Bonus").setMaster("local[2]")
    val sc = new SparkContext(conf)

    if (args.length < 1) {
      println("Please pass the input: <input_file>")
      System.exit(0)
    }


    var data= sc.textFile(args(0))
    var header_info = data.first()
    data = data.filter(line => line!=header_info).cache()
    var filtered_user_mov_1 =data.map(_.split(",")).map(line => (line(0).toInt,line(1).toInt)).groupByKey()
    var filtered_user_mov = filtered_user_mov_1.map{case (user,movie_list)=> (user,movie_list.toSet)}.filter{case (users,list_of_movies) => list_of_movies.size>=9}
    var filtered_users_1 = data.map(_.split(",")).map(line => (line(0).toInt,1))
    var filtered_users = filtered_users_1.distinct().map{case (user,count) => user}.collect.toList
    val map:Map[Int,scala.collection.immutable.Set[Int]] = filtered_user_mov.collect().toMap
    var comm_users =ListBuffer[(Long,Long)]()

    for (x <- filtered_users){
      for (y<- filtered_users){
        if(x!=y)
          if ((map(x).intersect(map(y))).size>=9)
            comm_users +=((x,y))
      }
    }

    var neigh_user_nodes_rdd  = sc.parallelize(comm_users)
    var neigh_user_nodes = neigh_user_nodes_rdd.groupByKey().map{case (user,next) => (user,next.toSet)}.collect.toMap
    var edge_array = comm_users.map{ case (x,y) => Edge(x,y,0.0)}.toArray
    var edge_rdd : RDD[Edge[Double]] = sc.parallelize(edge_array)
    val user_nodes_rdd : RDD[(VertexId,Int)] = sc.parallelize(filtered_users.map{case a => ((a.toLong,a.toInt))})
    var community_graph = Graph(user_nodes_rdd,edge_rdd)

    val sub_comp = community_graph.PSCAN(epsilon=0.75)
    val comp = sub_comp.vertices.map(vert => vert._2-> vert._1).groupByKey().map(v => (v._1,v._2.toList.sorted)).map(x => x._2)

    val final_result = comp.coalesce(1).saveAsTextFile("Prashanth_Manja_Bonus_Community_scala_output.txt")
  }
}
