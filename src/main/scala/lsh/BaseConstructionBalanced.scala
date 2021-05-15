package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstructionBalanced(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int, partitions : Int) extends Construction {
  //build buckets here
  val minHash = new MinHash(seed)
  val buckets = minHash.execute(data)
    .groupBy(movie_and_minhash =>movie_and_minhash._2)   // (hash, [(movie, hash)])
    .mapValues(ts => ts.map(t=>t._1))
  //var boundaries:Array[Int] = Array()
 // var numQuery:Int = 0

  def computeMinHashHistogram(queries : RDD[(String, Int)]) : Array[(Int, Int)] = {

    //numQuery = queries.count.toInt
    //println(s"numQueryGroundTruth = ${queries.count.toInt}")
    //compute histogram for target buckets of queries
    queries.groupBy(query_bucketId=>query_bucketId._2) // groupby bucketId
      .mapValues(query_bucketIds=>query_bucketIds.size)  // get number of queries for each bucket
      .sortByKey(ascending = true)    // sort by bucketid to prepare for partitioning
      .collect()
  }

  // note that histogram is (bucId, queryNum) sorted by bucId
  def computePartitions(histogram : Array[(Int, Int)]) : Array[Int] = {
    //compute the boundaries of bucket partitions
    val numQuery = histogram.map({case(bucId, queryNum)=>queryNum}).sum
    val thresh = math.ceil(numQuery.toDouble/partitions)
    var currPartitionSize = 0
    var boundaries = Array(histogram(0)._1) // add smallest bucketId as starting boundary for the first partition
    // bucket_size denotes tuple (bucket, size)
    for (bucId_bucSize <- histogram) {
      if (  (bucId_bucSize._2 + currPartitionSize) > thresh  ) {
        // if added in curr bucket, the partition size goes above threshold, then set curr bucket id as boundary
        boundaries = boundaries :+ bucId_bucSize._1
        currPartitionSize = 0
      } else {
        currPartitionSize += bucId_bucSize._2
      }
    }
    boundaries
  }

  // bid = bucket id
  // partition here is an abstract concept on top of physical partition
  // and we create RDD[(partitionId, (queries_bid, bid_bucket))]
  // and do near neighbour search in each partition auto-distributedly by spark
  // this is load balancing because load balance in all partitions, and partitions are further distributed by spark
  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    // empty query causes trouble e.g. when adding the smallest boundary when there's boundary
    if (queries.isEmpty) return queries.mapValues(lsStr => lsStr.toSet)

    //compute near neighbors with load balancing here
    // arrange query and buckets to appropriate partitions
    val query_bid = minHash.execute(queries)
    val queryHist = computeMinHashHistogram(query_bid)
    val boundaries = computePartitions(queryHist)

    // partition0 = [boundary0, boundary1), partition1 = [boundary1, boundary2), ... ,[lastboundary, largestBucId]
    val bucketId_to_partitionId = (boundaries:Array[Int], bucketId:Int) => {
      for (i <- boundaries.indices) {
        if (bucketId >= boundaries(i)) {
          i
        }
      }
      0 // dummy output, this should never happen as boundary(0) is the smallest bucketid
    }

    // group queries and data buckets by partitionId
    val queriesByParti = query_bid
      .map(q_bid => (bucketId_to_partitionId(boundaries, q_bid._2), q_bid))
      .groupByKey()
    val bucketsByParti = buckets
      .map({bid_b => (bucketId_to_partitionId(boundaries,bid_b._1), bid_b)})
      .groupByKey()


    // find neighbours for all queries in one partition
    val evalInOneParti = ( queries_buckets: (Iterable[(String, Int)],
      Iterable[(Int, Iterable[String])]) ) => {
      val (queries, buckets) = (queries_buckets._1, queries_buckets._2)
      for (q_bid <- queries; bid_b <- buckets; if q_bid._2==bid_b._1)
        yield (q_bid._1,bid_b._2.toSet)
    }
    // find neighbours for all queries in all partitions
    queriesByParti.join(bucketsByParti)  // RDD[(partitionId, (queries_bid, bid_bucket))]
      .values.flatMap(evalInOneParti)

  }

}