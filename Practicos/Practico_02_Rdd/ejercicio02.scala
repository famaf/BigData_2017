/*
Ejercicio 2
===========
Hacer un programa que cuente las palabras del archivo *README.md*.
*/

val inputRDD = sc.textFile("./../../zeppelin-0.7.2-bin-all/README.md") // RDD
val stOrConfRDD = inputRDD.flatMap(line => line.split(" ")) // se crea un nuevo RDD
stOrConfRDD.count()
