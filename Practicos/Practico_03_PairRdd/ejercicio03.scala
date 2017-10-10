/*
Ejercicio 3
===========
Repita el ejercicio anterior usando union
*/

// ============================================================================
/* Version Hernan Maina */
// val rdd1 = sc.parallelize(List(("a",1),("b",4)))
// val rdd2 = sc.parallelize(List(("a",2),("b",2), ("c",5), ("a",7)))
// val rdd3 = sc.parallelize(List(("a",2),("b",2), ("c",5)))
// val rdd_union = rdd1.union(rdd2).union(rdd3).reduceByKey((accum,y) => accum + y)
// rdd_union.collect.foreach(println)
// ============================================================================

val rdd1 = sc.parallelize(Seq( ("a",1), ("b",4) ))
val rdd2 = sc.parallelize(Seq( ("a",2), ("b",2), ("c",5), ("a",7) ))
val rdd3 = sc.parallelize(Seq( ("a",2), ("b",2), ("c",5) ))

val g_1 = rdd1.union(rdd2)
val g_2 = g_1.union(rdd3)
val gg = g_2.reduceByKey(_ + _)
//g_2.collect()
gg.collect()
grouped_union.collect.foreach(println)


//val grouped_list = grouped.mapValues(cb => (cb._1.toList, cb._2.toList, cb._3.toList))

//grouped_list.collect()

//val grouped_union = grouped_list.mapValues(x => x.productIterator.toList.asInstanceOf[List[List[Int]]].flatten.sum)
//val grouped_union = grouped_list.mapValues(x => (x._1 ++ x._2 ++ x._3).sum)
//grouped_union.collect()
