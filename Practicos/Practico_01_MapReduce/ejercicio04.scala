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
Ejercicio 4 (word co-ocurrencia)
================================
En el siguiente ejercicio hay que construir la matriz de co-ocurrencia de
palabras en una misma linea. Esta es una matriz simétrica "n*n" donde "n" es el
número de palabras (sin repetición) en un texto. Para cada par de palabras
(fila y columna de la matriz) se cuenta la cantidad de veces que ocurren ambas
en una misma linea.

Ayuda:
Se puede hacer que la función "fmap" devuelva los pares ordenados de palabras
en una misma linea con un contador igual a "1".
Por ejemplo, en la linea "w1 w2 w3 w1" la función producirá:
(w1, w2):1, (w1, w3):1, (w1, w1):1, (w2, w3):1, (w1, w2):1, (w1, w3):1

La función "freduce" recolectaría estos valores para llenar cada elemento de
la matriz.

A continuación se da un esqueleto del programa a completar:
*/

def wordCoOcurrence (filePath: String) = {
    import scala.io.Source

    val lines: List[String] = Source.fromFile(filePath).getLines.toList
    val datos = lines.map(l => ((), l))

    def fmap (unit: Unit, l: String) : List[((String, String), Int)] = {
        import collection.mutable

        val word_list = l.split(" ").filter(! _.isEmpty)
        val pair_list = mutable.ListBuffer[(String, String)]()

        for (i <- 0 until word_list.length) {
            for (j <- 0 until word_list.length) {
                if (i < j) {
                    pair_list += ((word_list(i), word_list(j)))
                }
            }
        }

        return pair_list.toList.map((_, 1))
    }

    val freduce = (pair_list: (String, String), vs: List[Int]) => (pair_list, vs.fold (0) (_+_))

    mapReduce (datos) (fmap) (freduce)
}

var FULL_PATH = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/zeppelin-0.7.3-bin-all/doc/"
val word_coocurence = wordCoOcurrence(FULL_PATH + "text_practico01.txt")
println(word_coocurence)


/* Version Elian y Joni */
// def wordCoOcurrence (filePath: String) = {
//     import scala.io.Source
//     val lines : List[String] = Source.fromFile(filePath).getLines.toList
//     val words = lines.map(l => ((), l.split(" ").toList))

//     val fmap = (_: Unit, line:List[String]) => (for (i <- 0 until line.length) yield for (j <- i+1 until line.length) yield ((line(i), line(j)), 1)).toList.flatten
//     val freduce = (coOcurr: (String, String), vs:List[Int]) => (coOcurr, vs.fold (0) (_+_))

//     mapReduce (words) (fmap) (freduce)
// }
// wordCoOcurrence("/home/jonathan/Desktop/prueba.txt")
// // val res = fmap((), l)
