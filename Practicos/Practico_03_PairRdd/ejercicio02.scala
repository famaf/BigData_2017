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
