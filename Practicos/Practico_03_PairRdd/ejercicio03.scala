/*
Ejercicio 3
===========
Repita el ejercicio anterior usando union
*/

val rdd1 = sc.parallelize(Seq( ("a",1), ("b",4) ))
val rdd2 = sc.parallelize(Seq( ("a",2), ("b",2), ("c",5), ("a",7) ))
val rdd3 = sc.parallelize(Seq( ("a",2), ("b",2), ("c",5) ))

val g_1 = rdd1.union(rdd2)
val g_2 = g_1.union(rdd3)
val gg = g_2.reduceByKey(_ + _)
//g_2.collect()
gg.collect()


//val grouped_list = grouped.mapValues(cb => (cb._1.toList, cb._2.toList, cb._3.toList))

//grouped_list.collect()

//val grouped_union = grouped_list.mapValues(x => x.productIterator.toList.asInstanceOf[List[List[Int]]].flatten.sum)
//val grouped_union = grouped_list.mapValues(x => (x._1 ++ x._2 ++ x._3).sum)
//grouped_union.collect()
