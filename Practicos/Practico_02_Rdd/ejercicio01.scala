/*
Ejercicio 1
===========
En la presentación de rdd se mostró un programa que filtra las apariciones de
las palabras “config” y “status”:

val inputRDD = sc.textFile("/doc/log.txt") // RDD
val statusRDD = inputRDD.filter(line => line.contains("ERROR")) // se crea un nuevo RDD
val configRDD = inputRDD.filter(line => line.contains("config")) // se crea un nuevo RDD
val stOrConfRDD = statusRDD.union(configRDD) 

Esta solución puede ser poco eficiente ya que el archivo se recorre dos veces.
Hacer un programa que recorra el archivo solo una vez filtrando ambas apariciones al mismo tiempo.
Comprobar la mejora viendo el grafo en la SparkUI.
*/

val inputRDD = sc.textFile("./../../zeppelin-0.7.2-bin-all/doc/log.txt") // RDD
val stOrConfRDD = inputRDD.filter(line => line.contains("ERROR") || line.contains("config")) // se crea un nuevo RDD
stOrConfRDD.take(10).foreach(println)
