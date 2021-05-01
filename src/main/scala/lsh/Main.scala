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
      val input = sc
        .parallelize(List(
          "Star Wars|space|force|jedi|empire|lightsaber",
          "The Lord of the Rings|fantasy|hobbit|orcs|swords",
          "Ghost in the Shell|cyberpunk|anime|hacker"
        ))

      val rdd = input
        .map(x => x.split('|'))
        .map(x => (x(0), x.slice(1, x.size).toList))

      val minHash21 = new MinHash(21)
      val minHash22 = new MinHash(22)
      val minHash23 = new MinHash(23)

      assert(minHash21.execute(rdd).map(x => x._2).collect().toList.equals(List(99766, 4722, 53951)))
      assert(minHash22.execute(rdd).map(x => x._2).collect().toList.equals(List(67943, 31621, 27051)))
      assert(minHash23.execute(rdd).map(x => x._2).collect().toList.equals(List(10410, 14613, 28224)))

      println("minhash test: all assertions passed")
    }

    // todo maybe each test get such a block

    {
      //val corpus_file = new File(getClass.getResource("/corpus-1.csv/part-00000").getFile).getPath
      val corpus_file = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000"

      val rdd_corpus = sc
        .textFile(corpus_file)
        .map(x => x.toString.split('|'))
        .map(x => (x(0), x.slice(1, x.size).toList))

      //val query_file = new File(getClass.getResource("/queries-1-2.csv/part-00000").getFile).getPath
      val query_file = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2.csv/part-00000"


      val rdd_query = sc
        .textFile(query_file)
        .map(x => x.toString.split('|'))
        .map(x => (x(0), x.slice(1, x.size).toList))
        .sample(false, 0.05)

      val exact = new ExactNN(sqlContext, rdd_corpus, 0.3)

      val lsh =  new BaseConstructionBroadcast(sqlContext, rdd_corpus, 42)

      val ground = exact.eval(rdd_query)
      val res = lsh.eval(rdd_query)

      assert(Main.recall(ground, res) >= 0.8)
      assert(Main.precision(ground, res) >= 0.9)

      assert(res.count() == rdd_query.count())

      println("BaseConstructionBroadcastSmall test: all assertions passed")
    }


  }     
}
