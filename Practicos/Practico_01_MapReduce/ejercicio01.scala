import hola.mapReduce

/*
Ejercicio 1
===========
En la celda siguiente modifique el programa anterior para que tome un archivo
en vez de un string.
La idea es que el "map" trabaje sobre cada linea de texto
(no sobre cada caracter).
No se puede usar el programa anterior.
A continuación se muestra un esqueleto del programa que debe completar
programando las funciones "fmap" y "freduce":
*/

def countCharFile (filePath: String) = {

    import scala.io.Source

    val lines: List[String] = Source.fromFile(filePath).getLines.toList
    val datos = lines.map(l => ((), l))

    val fmap = (_: Unit, l: String) => (l.split("").map((_, 1))).toList
    val freduce = (l: String, vs: List[Int]) => (l, vs.fold (0) (_+_))

    mapReduce (datos) (fmap) (freduce)
}

var count_file = countCharFile("./my_text.txt")
println(count_file)
