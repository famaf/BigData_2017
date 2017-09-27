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


/* Elian y Joni */
// def promedio (nums: List[Double]) : Double = {
//     val datos = nums.map(n => ((), n))

//     val fmap = (_: Unit, num: Double) => List(((), (1, num)))
//     val freduce = (_: Unit, num: List[(Int, Double)]) => num.fold ((0,0.0)) ((x: (Int, Double), y: (Int, Double)) => (x._1 + y._1, x._2 + y._2))
//     val resMapRed = (mapReduce (datos) (fmap) (freduce))(0)
//     return resMapRed._2 / resMapRed._1
// }
// val a: List[Double] = List(1,2,3,4,5)
// val r = promedio(a)



// var mapeador : List[(Set[String], List[String])] = List()
// mapeador +:= (Set("A", "B"), List("A", "B", "C"))
// mapeador :+= (Set("B", "B"), List("A", "B", "C"))
// mapeador

// def fmap(persona:String, amigos:List[String])
//     : List[(Set[String], List[String])] = {
//     var mapeador : List[(Set[String], List[String])] = List()
//     for (i <- amigos) mapeador +:= (Set(persona, i), amigos)
//     mapeador
// }
