/*
Implementación naif del patrón MapReduce
========================================
A continuación se dará una implementación muy simple del patrón en Scala.
La misma corre solo en una máquina, sin file system distribuido y solo nos
servirá para practicar su uso.
Lea el código y trate de entenderlo. Comparelo también con la presentación en
clase del tema. Cualquier duda consulte un docente.
*/

// Funcion map comun pero que devuelve lista de pares (clave,valor)
def elMap[Kin, Vin, Kout, Vout]
    (datosIn: List[(Kin, Vin)])
    (fmap: (Kin, Vin) => List[(Kout, Vout)])
    : List[(Kout, Vout)]
    = datosIn.flatMap(kv => fmap(kv._1, kv._2)) // Cannot use just f, something wierd with implicits

// Agrupa los valores que tienen clave comun
def agrupa[Kout, Vout]
    (kvs: List[(Kout, Vout)])
    : Map[Kout, List[Vout]]
    = kvs.groupBy(_._1).mapValues(_.unzip._2)

//    = kvs.groupBy(_._1).mapValues(_.map(_.2))

// Para cada clave aplico una operacion a su lista de valores
def reduce[Kout, Vout, VFin]
    (kvss: Map[Kout, List[Vout]])
    (freduce: (Kout, List[Vout]) => VFin)
    : List[VFin]
    = kvss.map({case (k, vs) => freduce(k, vs)}).toList

//Junto todo
def mapReduce[Kin, Vin, Kout, Vout, VFin]
    (datosIn: List[(Kin, Vin)])
    (fmap: (Kin, Vin) => List[(Kout, Vout)])
    (freduce: (Kout,  List[Vout]) => VFin)
    : List[VFin]
    = {
        val resMap = elMap (datosIn) (fmap)
        val resAgrupo = agrupa(resMap)
        val resReduce = reduce (resAgrupo) (freduce)
        return resReduce
    }

/*
Pequeño ejemplo de uso
======================
A continuación se verá la implementación em MapReduce del algoritmo que
encuentra la cantidad de apariciones de cada letra (visto en clase).
Pruebe ejecutarlo llamando a la función countChar con un String cualquiera.
*/

// Cuento cantidad de veces que aparece cada letra
// ===============================================
def countChar (str: String) = {
    val datos = str.toList.map(c => ((), c))

    val fmap = (_ :Unit, c: Char) => List((c, 1))
    val freduce = (c: Char, vs: List[Int]) => (c, vs.fold (0) (_+_))

    mapReduce (datos) (fmap) (freduce)
}

// var count_string = countChar("Messi")
// println(count_string)

//=============================================================================

/*
Ejercicio 2 (wordCount)
=======================
Hacer un programa que calcule la cantidad de veces que aparece cada palabra
(no vacía) en un archivo.
A continuación se muestra un esqueleto del programa que debe completar
programando las funciones "fmap" y "freduce".

Ayuda:
* Para dividir un String en palabras se puede usar el método "split".
* Para filtrar elementos de una lista se puede usar el método "filter".
* Para ver si un String no es vacío se puede usar "! _.isEmpty"
*/

def wordCount (filePath: String) = {
    import scala.io.Source

    val lines: List[String] = Source.fromFile(filePath).getLines.toList
    val datos = lines.map(l => ((), l))

    // Otra forma
    //val fmap = (_: Unit, l: String) => (l.split(" +").map((_, 1))).toList
    val fmap = (_: Unit, l: String) => (l.split(" ").filter(! _.isEmpty).map((_, 1))).toList
    val freduce = (w: String, vs: List[Int]) => (w, vs.fold (0) (_+_))

    mapReduce (datos) (fmap) (freduce)
}

var count_word = wordCount("my_text.txt")
println(count_word)


/* Elian y Joni */
// def wordCount (filePath: String) = {
//     import scala.io.Source
//     val lines : List[String] = Source.fromFile(filePath).getLines.toList
//     val datos = lines.map(l => ((),l))

//     val fmap = (_ : Unit, l : String) => l.split(" ").toList.map(s => (s, 1))
//     val freduce = (w: String, vs: List[Int]) => (w, vs.fold (0) (_+_))

//     mapReduce (datos) (fmap) (freduce)
// }
// wordCount("/home/jonathan/Desktop/prueba.txt")
