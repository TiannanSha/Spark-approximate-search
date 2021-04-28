package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction {
  //build buckets here
  val minHash = new MinHash(seed)
  val buckets = minHash.execute(data)
    .groupBy(movie_and_minhash =>movie_and_minhash._2)   // (hash, [(movie, hash)])
    .mapValues(ts => ts.map(t=>t._1).toSet)

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here
    minHash.execute(queries)        // hash all queries
      .map({case(movie,hashVal) => (hashVal, movie)})   // use hash value as key to join with buckets
      .join(buckets)  // (hash, (query, [data points in bucket with same hash]))
      .values
  }
}
