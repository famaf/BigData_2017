/*
Ejercicio 4
===========
Contar la cantidad de veces que aparece la letra ‘c’ en el archivo *README.md*.
*/

val inputRDD = sc.textFile("./../../zeppelin-0.7.2-bin-all/README.md") // RDD
val stOrConfRDD = inputRDD.flatMap(line => line.split("")).filter(letra => letra == "c") // se crea un nuevo RDD
stOrConfRDD.count()
