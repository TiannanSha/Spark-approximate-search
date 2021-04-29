package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction with Serializable {
  //build buckets here
  val minHash = new MinHash(seed)
  //val buckets = minHash.execute(data).collect().groupBy(movie_bid=>movie_bid._2)
  val buckets = minHash.execute(data)
    .groupBy(movie_and_minhash =>movie_and_minhash._2)   // (hash, [(movie, hash)])
    .mapValues(ts => ts.map(t=>t._1).toSet).collect().toMap
  val bcBuckets = sqlContext.sparkContext.broadcast(buckets)
//    .groupBy(movie_and_minhash =>movie_and_minhash._2)   // (hash, [(movie, hash)])
//    .mapValues(ts => ts.map(t=>t._1).toSet)

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here
    val query_bid = minHash.execute(queries)
    val evalOneQuery = (query_bid:(String, Int), buckets:Map[Int, Set[String]]) => {
      (query_bid._1, buckets(query_bid._2))
    }
    query_bid.map(query => evalOneQuery(query, bcBuckets.value))
  }
}
