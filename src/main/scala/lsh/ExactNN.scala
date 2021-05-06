package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold : Double) extends Construction with Serializable {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute exact near neighbors here

    //data is data all the data user is querying on; rdd is user's query

    // add an unique id for each query for later grouping. n = neighbour
    val rdd_query_qid = rdd.zipWithUniqueId()
    // fixme probably should zip unique on rdd first
    val result = rdd_query_qid.cartesian(data)  // ((query, qid), data)
      .filter( t => jaccard(t._1._1._2.toSet, t._2._2.toSet) > threshold )
      .map({case(((q,qWords),qid),(n, nWords)) => ((qid,q),n)})
      .groupByKey
      .map({case((qid, q),ns) => (q, ns.toSet)})
//    val crossProd = data.cartesian(rdd)
//
//
//    val result = crossProd
//      .filter(dataAndQueries => jaccard(dataAndQueries._1._2.toSet, dataAndQueries._2._2.toSet) > threshold)  // (dataEntry, query) that were matched
//      .zipWithUniqueId
//      .map({case(((query, qwords),(neighbour, dwords)),qid) => ((qid, query), neighbour)})
//      .groupByKey  //groupby queryId
//      .map({case((qid,query),neighbours) => (query, neighbours.toSet)})
       //group by qid  (query, [(dataEntry, query)])
      //.map({case(qid, query_data)=> (query_data._1._1, ts.map(t=>t._1._1).toSet)})
//    println("my result")
//    result.take(3).foreach(println)
    result
  }

  def jaccard(A:Set[String], B:Set[String]) = {
    val intersectSize = (A&B).size.toDouble
    intersectSize / (A.size + B.size - intersectSize)
  }
}
