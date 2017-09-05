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

// var count_file = countCharFile("./my_text.txt")
// println(count_file)

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

// var count_word = wordCount("./my_text.txt")
// println(count_word)

//=============================================================================

/*
Ejercicio 3 (amigos en común)
=============================
Dada una lista de tuplas, donde el primer elemento es una persona y el segundo
una lista de sus amigos, hacer un programa con "mapReduce" que devuelve la
lista de amigos en común de todos los pares de amigos posibles.

La lista de amigos se almacena de la siguiente forma:
val amigosDe = List(("A", List("B", "C", "D")),
                    ("B", List("A", "C", "D", "E")),
                    ("C", List("A", "B", "D", "E")),
                    ("D", List("A", "B", "C", "E")),
                    ("E", List("B", "C", "D"))
                   )

Ver que la relación de amistad tiene que ser simétrica.

Ayuda:
Para cada par "(p, ams)" de la lista de entrada la función "map" puede devolver
todas las tuplas posible "({p, am}, ams)" donde "{p, am}" es un conjunto de dos
persona y "am" es un elemento de "ams".
Ver que en estos resultados deben aparecer exactamente dos tuplas cuyos
primeros elementos (conjunto "{p, am}") son iguales.
A partir de esta observación los amigos en común son la intersección de las
segundas componentes de ambas tuplas.
A continuación se da un esqueleto del programa a completar:
*/

def amigosEnComun(ade: List[(String, List[String])]) = {
    import collection.mutable

    val datos = ade.map(a => ((), a))

    def fmap (unit: Unit, friends: (String, List[String]))
             : List[(scala.collection.immutable.Set[String], List[String])] = {
        val my_list = mutable.ListBuffer[(Set[String], List[String])]()

        val common_friend = friends._1
        val friends_list = friends._2

        for (f <- friends_list) {
            my_list += ((Set(common_friend, f), friends_list))
        }

        return my_list.toList
    }

    def freduce (pair_friends: scala.collection.immutable.Set[String],
                 list_friends: List[List[String]])
                : (scala.collection.immutable.Set[String], List[String]) = {
        val list_friends_set = list_friends.map(_.toSet)
        val initial_set = list_friends_set(0)
        val common_friends = list_friends_set.fold (initial_set) (_&_)

        return (pair_friends, common_friends.toList)
    }

    mapReduce (datos) (fmap) (freduce)
}

val amigosDe = List(("A", List("B", "C", "D")),
                    ("B", List("A", "C", "D", "E")),
                    ("C", List("A", "B", "D", "E")),
                    ("D", List("A", "B", "C", "E")),
                    ("E", List("B", "C", "D"))
                   )
// var my_friends = amigosEnComun(amigosDe)
// println(my_friends)


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

// val word_coocurence = wordCoOcurrence("my_text.txt")
// println(word_coocurence)

//=============================================================================

/*
Ejercicio 5 (promedio)
======================
Con el programa "mapReduce" calcule el promedio de una lista de numeros.

A continuación se da un esqueleto del programa a completar:
*/

def promedio (nums: List[Double]) : Double = {
    val datos = nums.map(n => ((), n))

    val fmap = (_ :Unit, n: Double) => List((n, 1))
    val freduce = (n: Double, vs: List[Int]) => (n, vs.fold (0) (_+_))

    val mr = mapReduce (datos) (fmap) (freduce)

    //> mr
    //> List[(Double, Int)] = List((num, ocurrences))
    return mr.map(_._1).sum / mr.map(_._2).sum
}

// var average = promedio(List(1.0, 2.0, 3.0, 4.0, 5.0))
var average = promedio(List(1.0, 1.0, 2.0, 2.0, 4.0))
// (1, 2) (2, 2) (4, 1)
println(average)
