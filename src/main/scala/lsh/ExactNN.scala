package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold : Double) extends Construction with Serializable {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute exact near neighbors here

    //data is data all the data user is querying on
    // rdd is user's query

    val crossProd = data.cartesian(rdd)

    val result = crossProd.filter(
      dataAndQueries => jaccard(dataAndQueries._1._2.toSet, dataAndQueries._2._2.toSet) > threshold
    )  // (dataEntry, query) that were matched
      .groupBy(dataAndQueries => dataAndQueries._2) //group by queries  (query, [(dataEntry, query)])
      .map({case(query, ts)=> (query._1, ts.map(t=>t._1._1).toSet)})
//    println("my result")
//    result.take(3).foreach(println)
    result
  }

  def jaccard(A:Set[String], B:Set[String]) = {
    val intersectSize = (A&B).size.toDouble
    intersectSize / (A.size + B.size - intersectSize)
  }
}
