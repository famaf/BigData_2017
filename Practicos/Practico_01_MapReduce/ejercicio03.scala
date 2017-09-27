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

var my_friends = amigosEnComun(amigosDe)
println(my_friends)


/* Elian y Joni */
// def amigosEnComun(ade: List[(String,List[String])]) = {
//     // val res = ade.flatMap(x => for (name <- x._2) yield (Set(x._1, name), x._2))
//     // val fmap = (name: String, friends:List[String]) => for (name_friend <- friends) yield (Set(name, name_friend), friends)

//     val fmap = (name: String, friends:List[String]) => friends.map(name_friend => (Set(name, name_friend), friends))
//     val freduce = (friend:Set[String], friends:List[List[String]]) => (friend, friends(0).intersect(friends(1)))

//     mapReduce (ade) (fmap) (freduce)
// }

// val amigosDe = List(  ("A", List("B", "C", "D"))
//                     , ("B", List("A", "C", "D", "E"))
//                     , ("C", List("A", "B", "D", "E"))
//                     , ("D", List("A", "B", "C", "E"))
//                     , ("E", List("B", "C", "D"))      )

// amigosEnComun(amigosDe)
