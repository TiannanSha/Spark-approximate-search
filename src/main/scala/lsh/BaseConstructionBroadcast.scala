package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction with Serializable {
  //build buckets here. bid = bucketId = hash val
  val minHash = new MinHash(seed)

  val buckets = minHash.execute(data)
    .groupBy(movie_and_minhash =>movie_and_minhash._2)   // (hash, [(movie, hash)])
    .mapValues(ts => ts.map(t=>t._1).toSet).collect().toMap
  // broadcasted buckets
  val bcBuckets = sqlContext.sparkContext.broadcast(buckets)


  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here. bid = bucketId = hash val
    val query_bid = minHash.execute(queries)
    // find neighbours for one query
    val evalOneQuery = (query_bid:(String, Int), buckets:Map[Int, Set[String]]) => {
      (query_bid._1, buckets(query_bid._2))
    }
    // find neighbours for all queries
    query_bid.map(query => evalOneQuery(query, bcBuckets.value))
  }
}
