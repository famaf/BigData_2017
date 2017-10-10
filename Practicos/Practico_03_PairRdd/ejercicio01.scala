/*
Ejercicio 1
===========
Hacer un programa que calcule las 10 palabras, con mas de 3 caracteres, mas
frecuentes en el *README*.
*/

val FULL_PATH = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/zeppelin-0.7.3-bin-all/"
val file = sc.textFile(FULL_PATH + "README.md")

// En el filter poner que sean mas de 3 caracteres
val words = file.flatMap(_.split(" ")).filter(_.length > 3) // Le saca los vacios
val wordCount = words.map(x => (x,1)).reduceByKey((nx,ny) => nx+ny)
val result = wordCount.takeOrdered(10)(Ordering[Int].reverse.on (_._2))

println("%table\nWord\tCount")
result.foreach{case (w,c) => println("\"" + w + "\"" + "\t" + c)}
