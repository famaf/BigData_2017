/*
Ejercicio 4
===========
DBPedia es una síntesis estructurada de las relaciones que aparecen en la Wikipedia.

Estructura sus datos en 2 tipos de archivos:
    * Propiedades - Archivo que contiene 3-uplas con (sujeto,relación,predicado),
      por ejemplo (Aristoteles, Año de nacimiento, -384)
    * Tipos - Archivo que contiene 3-uplas con (sujeto, sintaxis de tipo, tipo del sujeto).

En los Archivos de prueba de la materia hay extractos de los mismos
(*mappingbased_properties_en.nt* y *instance_types_en.nt*).

Analizar el formato de estos archivos sanearlos para crear los RDD
teniendo en cuenta que:
    * No se pueden modificar a mano los archivos.
    * Puede haber lineas basura. Son las que no comienzan con el caracter *<*
    * Puede haber lineas en blanco.
    * Cada linea termina con un punto que hay que descartar.
    * Un sujeto puede aparecer en varias lineas con distintas
      relacion/predicado en *mappingbased_properties_en.nt*
    * Un sujeto puede tener varios tipos en *instance_types_en.nt*.
    * La sintaxis de tipo en el archivo *instance_types_en.nt* no hay
      que tenerla en cuenta en el algoritmo.

El programa debe producir una salida que agregue a las propiedades los tipos
del sujeto como una lista, de forma tal que cada linea tenga la información
de la 4-upla (sujeto, [tipos del sujeto], relación, predicado).
*/

/* Version Hernan Maina */
val FULL_PATH = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/zeppelin-0.7.3-bin-all/doc/"
val rdd_prop = sc.textFile(FULL_PATH + "mappingbased_properties_en.nt")
val rdd_types = sc.textFile(FULL_PATH + "instance_types_en.nt")

// ============== Limpio archivo "mappingbased_properties_en.nt" ==============
// Saco lineas basura (todo diferente de las que empiezan con '<')
val prop_clean = rdd_prop.filter(_.startsWith("<"))

// Transformo lineas en par key-value (sujeto, (relacion,predicado)) y saco "." al final
val prop_kv = prop_clean.map(_.stripSuffix(".").split(" ")).
                         map(l => (l(0), (l(1), l.tail.tail.mkString(" "))))


// ================== Limpio archivo "instance_types_en.nt" ===================
// Saco lineas basura (todo diferente de las que empiezan con '<')
val types_clean = rdd_types.filter(_.startsWith("<"))

// Transformo lineas en par key-value (sujeto, [tipos_del_sujeto])
// Nota : toSet.toList para que borre duplicados y se convierta en List
val types_kv = types_clean.map(_.split(" ")).
                           map(l => (l(0), l(2))).
                           groupByKey.map{ case(k,v) => (k, v.toSet.toList) }

// ============================= Resultado Final ==============================
// prop_kv.leftOuterJoin(types_kv): Array[( Sujeto,((Relacion, Predicado), Option[List[tipos]]))]
val rdd_result = prop_kv.leftOuterJoin(types_kv).
                         map{ case(s,((r,p),t)) => (s,t.getOrElse(List()),r,p) }

println("\n==== Salida ======\n")
// Muestro los primeros 10 sujetos
rdd_result.take(10).foreach(println)


// ============================== Test de conteo ==============================
// Controlo que tanto "prop_kv" como "rdd_result" posean la misma cantidad de lineas.
println("\n======== Test de Conteo ============\n")
val result_count = rdd_result.count
val prop_count = prop_kv.count

if (result_count == prop_count) {
    println("\n### Test Correcto !!! ###\n")
} else {
    println("\n### Error en Test ###\n")
}


// ========================= Test de sujetos sin tipo =========================
// Imprimo algunos <sujetos> que no posean ningun <tipo>
println("\n===== Test de control de <sujetos> sin <tipo> =====\n")
rdd_result.filter{case(s,t,r,p) => t == List()}.take(2).foreach(println)
