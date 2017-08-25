def agrupa[Kout, Vout]
    (kvs: List[(Kout, Vout)])
    : Map[Kout, List[Vout]]
    = kvs.groupBy(_._1).mapValues(_.unzip._2)

def elMap[Kin, Vin, Kout, Vout]
    (datosIn: List[(Kin, Vin)])
    (fmap: (Kin, Vin) => List[(Kout, Vout)])
    : List[(Kout, Vout)]
    = datosIn.flatMap(kv => fmap(kv._1, kv._2)) // Cannot use just f, something wierd with implicits

def fmap (unit: Unit, friends: List[(String, List[String])]) : List[(scala.collection.immutable.Set[String], List[String])] = {
    import collection.mutable

    val my_list = mutable.ListBuffer[( Set[String], List[String] )]()

    val comun_friend = friends(0)._1
    val friends_list = friends(0)._2

    for (f <- friends_list) {
        my_list += ((Set(comun_friend, f), friends_list))
    }

    return my_list.toList
}



val misAmigos = List(((), ("A", List("B", "C", "D"))), ((), ("C", List("D", "A"))))
val amigosDeA = List(("A", List("B", "C", "D")))
val amigosDeC = List(("C", List("D", "A")))
println(elMap (misAmigos) (fmap))
// println(fmap(amigosDeA))
// println(fmap(amigosDeC))

val x = List(List((Set("A", "B"),List("B", "C", "D")),
                  (Set("A", "C"),List("B", "C", "D")),
                  (Set("A", "D"),List("B", "C", "D"))),
             List((Set("C", "D"),List("D", "A")),
                  (Set("C", "A"),List("D", "A")))
                )


val amigos = List((Set("A", "B"),List("B", "C", "D")),
                  (Set("A", "C"),List("B", "C", "D")),
                  (Set("A", "D"),List("B", "C", "D")),
                  (Set("C", "D"),List("D", "A")),
                  (Set("C", "A"),List("D", "A"))
                )

/*
Map(Set(A, D) -> List(List(B, C, D)),
    Set(A, B) -> List(List(B, C, D)),
    Set(C, D) -> List(List(D, A)),
    Set(A, C) -> List(List(B, C, D), List(D, A)))
*/
println(agrupa(amigos))
