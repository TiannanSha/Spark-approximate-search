package lsh

import org.apache.spark.rdd.RDD

class ORConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute OR construction results here

    var subResults:List[RDD[(String, Set[String])]] = List()
    for (c <- children) {
      subResults = subResults :+ c.eval(rdd)
    }

    // sort two sub results on query first and then zip
    // if there are duplicate query q1, this ensures each q1 on match with one q1
    // suppose there are 3 duplicate queries q1, and hence 3 (q1,buc1) in subRes1 and 3 (q1,buc2) in subRes2
    // if we just do join, then result would have 9 (q1,(buc1, buc2))
    val combineTwoSubRes = (subRes1:RDD[(String, Set[String])], subRes2:RDD[(String, Set[String])]) => {
      subRes1.sortByKey()
        .zip(subRes2.sortByKey())
        .map({case((query1, bucket1),(query2, bucket2))=>(query1,(bucket1|bucket2))})
    }

    subResults.foldLeft(subResults(0))(combineTwoSubRes)
  }
}
