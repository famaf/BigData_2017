/*
Ejercicio 2
===========
Hacer un programa que cuente las palabras del archivo *README.md*.
*/

val FULL_PATH = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/zeppelin-0.7.3-bin-all/"
val inputRDD = sc.textFile(FULL_PATH + "README.md") // RDD
val stOrConfRDD = inputRDD.flatMap(line => line.split(" ")) // se crea un nuevo RDD
stOrConfRDD.count()
