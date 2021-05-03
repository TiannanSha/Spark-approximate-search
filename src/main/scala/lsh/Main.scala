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
      // toy corpus and queries to test avg dist functions
      val raw_corpus = sc
        .parallelize(List(
          "Star Wars|space|force|jedi|empire|lightsaber",
          "The Lord of the Rings|fantasy|hobbit|orcs|swords",
          "Ghost in the Shell|cyberpunk|anime|hacker"
        ))
      val rdd_corpus = raw_corpus
        .map(x => x.split('|'))
        .map(x => (x(0), x.slice(1, x.size).toList))

      val raw_query = sc
        .parallelize(List(
          "Star Wars|space|force|jedi|empire|lightsaber",
          "The Lord of the Rings|fantasy|hobbit|orcs|swords",
          "Ghost in the Shell|cyberpunk|romance|orcs"
        ))
      val rdd_query = raw_query
        .map(x => x.split('|'))
        .map(x => (x(0), x.slice(1, x.size).toList))

      val bc = new BaseConstruction(sqlContext, rdd_query, 42)
      val res = bc.eval(rdd_query)

//      val corpus_file = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000"
//      val rdd_corpus = sc
//        .textFile(corpus_file)
//        .map(x => x.toString.split('|'))
//        .map(x => (x(0), x.slice(1, x.size).toList))

//      val query_file = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2.csv/part-00000"
//      val rdd_query = sc
//        .textFile(query_file)
//        .map(x => x.toString.split('|'))
//        .map(x => (x(0), x.slice(1, x.size).toList))
//        .sample(false, 0.5)

      val exact = new ExactNN(sqlContext, rdd_corpus, 0.3)
      val t_exact = System.nanoTime()
      val ground = exact.eval(rdd_query)
      ground.count() // use action to triger spark execution
      val duration_exact = (System.nanoTime() - t_exact)/1e9d
      println(s"ExactNN duration = $duration_exact")
      val avgAvgDist_exact = avgAvgDist(ground, rdd_query, rdd_corpus)
      println(s"avgAvgDist_exact = $avgAvgDist_exact")
      println("---------------------------")

      val lsh_base =  new BaseConstruction(sqlContext, rdd_corpus, 42)
      val t_base = System.nanoTime()
      val res_base = lsh_base.eval(rdd_query)
      res_base.count()
      val duration_base = (System.nanoTime()-t_base)/1e9d
      println(s"Base duration = $duration_base")
      val avgAvgDist_base = avgAvgDist(res_base, rdd_query, rdd_corpus)
      println(s"avgAvgDist_base = $avgAvgDist_base")

      val lsh_balanced =  new BaseConstructionBalanced(sqlContext, rdd_corpus, 42, 8)
      val t_balanced = System.nanoTime()
      val res_balanced = lsh_balanced.eval(rdd_query)
      res_balanced.count
      val duration_balanced = (System.nanoTime()-t_balanced)/1e9d
      println(s"Balanced duration = $duration_balanced")
      val avgAvgDist_balanced = avgAvgDist(res_balanced, rdd_query, rdd_corpus)
      println(s"avgAvgDist_balanced = $avgAvgDist_balanced")

      val bc_lsh =  new BaseConstructionBroadcast(sqlContext, rdd_corpus, 42)
      val t_bc = System.nanoTime()
      val res_bc = bc_lsh.eval(rdd_query)
      res_bc.count // use an action to triger spark execution to time
      val duration_bc = (System.nanoTime()-t_bc)/1e9d
      println(s"Broadcast duration = $duration_bc")
      val avgAvgDist_bc = avgAvgDist(res_bc, rdd_query, rdd_corpus)
      println(s"avgAvgDist_bc = $avgAvgDist_bc")

//      assert(Main.recall(ground, res_bc) >= 0.8)
//      assert(Main.precision(ground, res_bc) >= 0.9)
//
//      assert(res_bc.count() == rdd_query.count())
//
//      println("BaseConstructionBroadcastSmall test: all assertions passed")
    }


  }

  // calculate average distance between each query to its neighbours,
  def avgAvgDist(res:  RDD[(String, Set[String])],
                rdd_query: RDD[(String, List[String])], rdd_corpus: RDD[(String, List[String])]): Double={
    println("result")
    res.collect.foreach(println)
    val map_query = rdd_query.collect.toMap
    val map_corpus = rdd_corpus.collect.toMap
    res.map({case(query, neighbours) => avgDist(query, neighbours, map_query, map_corpus)}).mean
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
    println(s"A=$A")
    println(s"B=$B")
    val intersectSize = (A&B).size.toDouble
    intersectSize / (A.size + B.size - intersectSize)
  }

}
