/*
Ejercicio 6
===========
Dado el archivo *wikipedia_short.xml* contar las cantidad de lineas que tienen
la palabra “human”, las que tienen la palabra “activity” y las que tienen ambas
palabras utilizando *intersection* (puede suponer que no hay lineas repetidas).

Probar hacer el programa sin y con persistencia en memoria y comparar los
resultados utilizando Spark UI.
*/

val FULL_PATH = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/zeppelin-0.7.3-bin-all/doc/"
val inputRDD = sc.textFile(FULL_PATH + "wikipedia_short.xml") // RDD

val RDD1 = inputRDD.filter(line => line.contains("human")) // se crea un nuevo RDD
val RDD2 = inputRDD.filter(line => line.contains("activity")) // se crea un nuevo RDD
val RDDI = RDD1.intersection(RDD2)
RDD1.count()
RDD2.count()
RDDI.count()

// val FULL_PATH "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/zeppelin-0.7.3-bin-all/doc/"
// val inputRDD = sc.textFile(FULL_PATH + "wikipedia_short.xml") // RDD

// val RDD1 = inputRDD.filter(line => line.contains("human")).cache() // se crea un nuevo RDD
// val RDD2 = inputRDD.filter(line => line.contains("activity")).cache() // se crea un nuevo RDD
// val RDDI = RDD1.intersection(RDD2).cache()
// RDD1.count()
// RDD2.count()
// RDDI.count()
