package lsh

import org.apache.spark.rdd.RDD

class ORConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute OR construction results here

    // sort sub results on query first and then assign each query a qid
    // if there are duplicate query q1, this ensures each q1 on match with exactly one q1
    // suppose there are 3 duplicate queries q1, and hence 3 (q1,buc1) in subRes1 and 3 (q1,buc2) in subRes2
    // if we just do join, then result would have 9 (q1,(buc1, buc2))
    val subResults = children.map(c => c.eval(rdd).sortByKey().zipWithIndex().map({case((q,ns),qid)=>(qid, (q, ns))}))

    // combine two sub-results
    val combineTwoSubRes = (subRes1:RDD[(Long, (String, Set[String]))], subRes2:RDD[(Long, (String, Set[String]))]) => {
      subRes1.join(subRes2)
        .map(  {case( qid, ((q1,ns1),(q2,ns2)) ) => (qid, (q1, ns1|ns2))}  )
    }
    // combine all sub-results
    subResults.reduce(combineTwoSubRes).values
  }
}
