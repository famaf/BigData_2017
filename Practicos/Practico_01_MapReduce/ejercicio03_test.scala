def agrupa[Kout, Vout]
    (kvs: List[(Kout, Vout)])
    : Map[Kout, List[Vout]]
    = kvs.groupBy(_._1).mapValues(_.unzip._2)

def elMap[Kin, Vin, Kout, Vout]
    (datosIn: List[(Kin, Vin)])
    (fmap: (Kin, Vin) => List[(Kout, Vout)])
    : List[(Kout, Vout)]
    = datosIn.flatMap(kv => fmap(kv._1, kv._2)) // Cannot use just f, something wierd with implicits

def reduce[Kout, Vout, VFin]
    (kvss: Map[Kout, List[Vout]])
    (freduce: (Kout, List[Vout]) => VFin)
    : List[VFin]
    = kvss.map({case (k, vs) => freduce(k, vs)}).toList


def fmap (unit: Unit, friends: (String, List[String]))
         : List[(scala.collection.immutable.Set[String], List[String])] = {
    import collection.mutable

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
    import collection.mutable

    val list_friends_set = list_friends.map(_.toSet)
    val initial_set = list_friends_set(0)
    val common_list_friends = list_friends_set.fold (initial_set) (_&_)

    return (pair_friends, common_list_friends.toList)
}


val amigosDe = List(("A", List("B", "C", "D")),
                    ("B", List("A", "C", "D", "E")),
                    ("C", List("A", "B", "D", "E")),
                    ("D", List("A", "B", "C", "E")),
                    ("E", List("B", "C", "D"))
                   )

val misAmigos = List(("A", List("B", "C", "D")),
                     ("C", List("D", "A"))
                    )
// println(misAmigos)
/*
List( ("A", List("B", "C", "D")),
      ("C", List("D", "A"))
    )
*/

// val misAmigos_2 = amigosDe.map(a => ((), a))
val misAmigos_2 = misAmigos.map(a => ((), a))
// println(misAmigos_2)
/*
List( ((), (A, List(B, C, D))),
      ((), (C, List(D, A)))
    )
*/

// println(fmap((), ("A", List("B", "C", "D"))))
/*
>>
List( (Set(A, B), List(B, C, D)),
      (Set(A, C), List(B, C, D)),
      (Set(A, D), List(B, C, D))
    )
*/

val misAmigos_3 = elMap (misAmigos_2) (fmap)
// println(misAmigos_3)
/*
List( (Set(A, B), List(B, C, D)),
      (Set(A, C), List(B, C, D)),
      (Set(A, D), List(B, C, D)),
      (Set(C, D), List(D, A)),
      (Set(C, A), List(D, A))
    )
*/
/*
List( (Set(A, B), List(B, C, D)),
      (Set(A, C), List(B, C, D), List(D, A)),
      (Set(A, D), List(B, C, D)),
      (Set(C, D), List(D, A))
    )
*/

val misAmigos_4 = agrupa (misAmigos_3)
// println(misAmigos_4)
/*
Map( Set(A, D) -> List(List(B, C, D)),
     Set(A, B) -> List(List(B, C, D)),
     Set(C, D) -> List(List(D, A)),
     Set(A, C) -> List(List(B, C, D), List(D, A))
    )
*/
// HASTA ACA TODO BIEN

val resReduce = reduce (misAmigos_4) (freduce)
println(resReduce)
