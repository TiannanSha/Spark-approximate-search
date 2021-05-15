package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File


object Main {
  def generate(sc : SparkContext, input_file : String, output_file : String, fraction : Double) : Unit = {
    val rdd_corpus = sc
      .textFile(input_file)
      .sample(false, fraction)

    rdd_corpus.coalesce(1).saveAsTextFile(output_file)
  }

  def recall(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val recall_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._1.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_recall = recall_vec._1/recall_vec._2

    avg_recall
  }

  def precision(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val precision_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._2.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_precision = precision_vec._1/precision_vec._2

    avg_precision
  }

  def construction1(SQLContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    //implement construction1 composition here
    // goal is to have precision greater than 0.94
    val baseCon0 = new BaseConstruction(SQLContext, rdd_corpus, 0)
    val baseCon1 = new BaseConstruction(SQLContext, rdd_corpus, 1)
    val baseCon2 = new BaseConstruction(SQLContext, rdd_corpus, 2)
    val lsCons = List(baseCon0, baseCon1, baseCon2)
    new ANDConstruction(lsCons)
  }

  def construction2(SQLContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    //implement construction2 composition here
    val baseCon0 = new BaseConstruction(SQLContext, rdd_corpus, 0)
    val baseCon1 = new BaseConstruction(SQLContext, rdd_corpus, 1)
    val baseCon2 = new BaseConstruction(SQLContext, rdd_corpus, 2)
    val baseCon3 = new BaseConstruction(SQLContext, rdd_corpus, 3)
    val lsCons = List(baseCon0, baseCon1, baseCon2, baseCon3)
    new ORConstruction(lsCons)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //type your queries here
    {

      //using actual corpus and query files
      val corpus_file = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000"
      val rdd_corpus = sc
        .textFile(corpus_file)
        .map(x => x.toString.split('|'))
        .map(x => (x(0), x.slice(1, x.size).toList))

      val q_file1 = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2.csv/part-00000"
      val q_file2 = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2-skew.csv/part-00000"
      val q_file3 = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10.csv/part-00000"
      val q_file4 = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10-skew.csv/part-00000"

      val query_file = q_file1
      val rdd_query = sc
        .textFile(query_file)
        .map(x => x.toString.split('|'))
        .map(x => (x(0), x.slice(1, x.size).toList))
        .sample(false, 0.05)

      println()
      println("**********************************")
      println(corpus_file)
      println(query_file)
      println("**********************************")

      val exact = new ExactNN(sqlContext, rdd_corpus, 0.3)
      val t_exact = System.nanoTime()
      val ground = exact.eval(rdd_query)
      ground.count() // use action to triger spark execution
      val duration_exact = (System.nanoTime() - t_exact)/1e9d
      println(s"ExactNN duration = $duration_exact")
      val avgAvgDist_exact = avgAvgDist(ground, rdd_query, rdd_corpus)
      println(s"avgAvgDist_exact = $avgAvgDist_exact")
      val precision_exact = precision(ground, ground)
      val recall_exact = recall(ground, ground)
      println(s"precision_exact = $precision_exact")
      println(s"recall_exact = $recall_exact")
      println("---------------------------")

      val lsh_base =  new BaseConstruction(sqlContext, rdd_corpus, 42)
      val t_base = System.nanoTime()
      val res_base = lsh_base.eval(rdd_query)
      res_base.count()
      val duration_base = (System.nanoTime()-t_base)/1e9d
      println(s"Base duration = $duration_base")
      val avgAvgDist_base = avgAvgDist(res_base, rdd_query, rdd_corpus)
      val precision_base = precision(ground, res_base)
      val recall_base = recall(ground, res_base)

      println("---------------------------")

      val lsh_balanced =  new BaseConstructionBalanced(sqlContext, rdd_corpus, 42, 8)
      val t_balanced = System.nanoTime()
      val res_balanced = lsh_balanced.eval(rdd_query)
      res_balanced.count
      val duration_balanced = (System.nanoTime()-t_balanced)/1e9d
      println(s"Balanced duration = $duration_balanced")
      val avgAvgDist_balanced = avgAvgDist(res_balanced, rdd_query, rdd_corpus)
      println(s"avgAvgDist_balanced = $avgAvgDist_balanced")
      val precision_balanced = precision(ground, res_balanced)
      val recall_balanced = recall(ground, res_balanced)
      println(s"precision_balanced = $precision_balanced")
      println(s"recall_balanced = $recall_balanced")
      println("---------------------------")

      val bc_lsh =  new BaseConstructionBroadcast(sqlContext, rdd_corpus, 42)
      val t_bc = System.nanoTime()
      val res_bc = bc_lsh.eval(rdd_query)
      res_bc.count // use an action to triger spark execution to time
      val duration_bc = (System.nanoTime()-t_bc)/1e9d
      println(s"Broadcast duration = $duration_bc")
      val avgAvgDist_bc = avgAvgDist(res_bc, rdd_query, rdd_corpus)
      println(s"avgAvgDist_bc = $avgAvgDist_bc")
      val precision_bc = precision(ground, res_bc)
      val recall_bc = recall(ground, res_bc)
      println(s"precision_bc = $precision_bc")
      println(s"recall_bc = $recall_bc")
      println("---------------------------")

      val bc_lsh_1 =  new BaseConstruction(sqlContext, rdd_corpus, 42)
      val bc_lsh_2 =  new BaseConstruction(sqlContext, rdd_corpus, 43)
      val bc_lsh_3 =  new BaseConstruction(sqlContext, rdd_corpus, 42)
      val bc_lsh_4 =  new BaseConstruction(sqlContext, rdd_corpus, 43)


      val ls_cons_1 = List(bc_lsh_1, bc_lsh_2)
      val ls_cons_2 = List(bc_lsh_3, bc_lsh_4)
      val lsh_and = new ANDConstruction(ls_cons_1)
      val lsh_or = new ORConstruction(ls_cons_2)

      val t_and = System.nanoTime()
      val res_and = lsh_and.eval(rdd_query)
      res_and.count
      val duration_and = (System.nanoTime() - t_and)/1e9d

      val t_or = System.nanoTime()
      val res_or = lsh_or.eval(rdd_query)
      res_or.count
      val duration_or = (System.nanoTime() - t_or)/1e9d

      val avgAvgDist_and = avgAvgDist(res_and, rdd_query, rdd_corpus)
      val avgAvgDist_or = avgAvgDist(res_or, rdd_query, rdd_corpus)

      println("----")
      println(s"precision_base = $precision_base")
      println(s"AndPrecision = ${precision(ground, res_and)}")
      println(s"OrPrecision = ${precision(ground, res_or)}")
      println("----")
      println(s"BaseRecall = $recall_base")
      println(s"AndRecall = ${recall(ground, res_and)}")
      println(s"OrRecall = ${recall(ground, res_or)}")
      println("----")
      println(s"AndDuration = ${duration_and}")
      println(s"OrDuration = ${duration_or}")
      println("----")
      println(s"avgAvgDist_exactNN = ${1 - avgAvgDist_exact}")
      println(s"avgAvgDist_base = ${1 - avgAvgDist_base}")
      println(s"avgAvgDist_and = ${1 - avgAvgDist_and}")
      println(s"avgAvgDist_or = ${1 - avgAvgDist_or}")


    }
  }

  // calculate average distance between each query to its neighbours,
  def avgAvgDist(res:  RDD[(String, Set[String])],
                rdd_query: RDD[(String, List[String])], rdd_corpus: RDD[(String, List[String])]): Double={
    // simple implementation which collects rdds to maps first
//    val map_query = rdd_query.collect.toMap
//    val map_corpus = rdd_corpus.collect.toMap
//    res.map({case(query, neighbours) => avgDist(query, neighbours, map_query, map_corpus)}).mean
    val rdd_query_distinct = rdd_query.distinct
    val rdd_corpus_distinct = rdd_corpus.distinct
    val temp =
      res.zipWithUniqueId() // create a unique id for each query, for later group sims by queryId
      .flatMap({case((query,neighbours),qid) => neighbours.map( n => (query,(qid,n)) )})
      .join(rdd_query_distinct)
    val temp1 =
      temp.map({case(  query, ((qid,n),queryWords)  ) => (n, (query,qid,queryWords))})
      .join(rdd_corpus)
      .map({case(n, ((query,qid,queryWords),dataWords)) => (qid, jaccard(queryWords.toSet, dataWords.toSet))})
      .groupByKey   // group all similarities by the unique query id
      .mapValues(simsOfAQuery => simsOfAQuery.sum/simsOfAQuery.size.toDouble) //each query's avg dist
      .values

    println(s"rdd_query.count = ${rdd_query.count}")
    println(s"res.count = ${res.count}")
    temp1.mean
  }

  def avgDist(query:String, neighbours:Set[String],
              map_query: Map[String, List[String]], map_corpus: Map[String, List[String]]): Double= {
    //val queryWords = rdd_query.lookup(query).head
    val dists = {
      for {n <- neighbours} yield jaccard(map_query(query).toSet, map_corpus(n).toSet)
    }
    dists.sum/dists.size
  }

  def jaccard(A:Set[String], B:Set[String]) = {
    val intersectSize = (A&B).size.toDouble
    intersectSize / (A.size + B.size - intersectSize)
  }

}
