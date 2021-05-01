package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


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

    println("all assertions passed")
  }     
}
