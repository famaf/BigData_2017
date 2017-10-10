/*
Ejercicio 5
===========
Devolver todos los links internos del archivo *wikipedia_short.xml*
(el archivo esta en el la página de la materia o en el directorio "/doc").

Pueden aparecer varios links en una lines y son de la forma *[[link]]* o *[[link|text]]*.
*/

val FULL_PATH = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/zeppelin-0.7.3-bin-all/doc/"
val inputRDD = sc.textFile(FULL_PATH + "wikipedia_short.xml") // RDD
val Pattern = raw"\[\[([^\]]+)\]\]".r
val stOrConfRDD = inputRDD.flatMap(Pattern.findAllIn(_).toList).map(x => x.slice(2,x.length()-2).split('|')(0)) // se crea un nuevo RDD
stOrConfRDD.take(30).foreach(println)
