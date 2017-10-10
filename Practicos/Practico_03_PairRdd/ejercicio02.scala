/*
Ejercicio 2
===========
Dada las siguientes collecciones de pares de elementos:

("a",1), ("b",4)
("a",2), ("b",2), ("c",5), ("a",7)
("a",2), ("b",2), ("c",5)

Calcule la suma total para cada key usando cogroup.

(a,12)
(b,8)
(c,10)
*/

// ============================================================================
/* Version 1: Hernan Maina */
// // Forma prolija. Con aggregate y mapValues
// val rdd1 = sc.parallelize(List(("a",1),("b",4)))
// val rdd2 = sc.parallelize(List(("a",2),("b",2), ("c",5), ("a",7)))
// val rdd3 = sc.parallelize(List(("a",2),("b",2), ("c",5)))

// val rdd_cogroup =   rdd1.cogroup(rdd2,rdd3).
//                     mapValues(v => List(v._1.toList,v._2.toList,v._3.toList))

// val rdd_res = rdd_cogroup.mapValues{_.aggregate(0)(
//                                         (acc,value) => acc + value.fold(0)(_+_),
//                                         (acc1,acc2) => acc1 + acc2)
//                                     }
// rdd_res.collect.foreach(println)
// ============================================================================

// ============================================================================
/* Version 2: Hernan Maina */
// // Otra forma . MUUUUUUUUUCHOS map :S
// val rdd1 = sc.parallelize(List(("a",1),("b",4)))
// val rdd2 = sc.parallelize(List(("a",2),("b",2), ("c",5), ("a",7)))
// val rdd3 = sc.parallelize(List(("a",2),("b",2), ("c",5)))
// val ddd = rdd1.cogroup(rdd2,rdd3)
//               .map{case(w,v) => (w,List(v._1.toList,v._2.toList,v._3.toList))}

// val ddd_toList = ddd.map{case(k,v) => (k,v.map(_.fold(0)(_+_)))}
//                     .map{case(k,v) => ( k,v.fold(0)(_+_).toString )}

// ddd_toList.collect.foreach(println)
// ============================================================================


val rdd1 = sc.parallelize(Seq( ("a",1), ("b",4) ))
val rdd2 = sc.parallelize(Seq( ("a",2), ("b",2), ("c",5), ("a",7) ))
val rdd3 = sc.parallelize(Seq( ("a",2), ("b",2), ("c",5) ))

val grouped = rdd1.cogroup(rdd2, rdd3)

grouped.collect()

val grouped_list = grouped.mapValues(cb => (cb._1.toList, cb._2.toList, cb._3.toList))

grouped_list.collect()

val grouped_union = grouped_list.mapValues(x => x.productIterator.toList.asInstanceOf[List[List[Int]]].flatten.sum)
//val grouped_union = grouped_list.mapValues(x => (x._1 ++ x._2 ++ x._3).sum)
grouped_union.collect()
grouped_union.collect.foreach(println)
