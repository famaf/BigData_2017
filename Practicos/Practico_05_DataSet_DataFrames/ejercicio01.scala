/*
Ejercicio 1 (problema con dataset de ejercicio de práctico anterior)
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
    * Ppuede haber lineas basura. Son las que no comienzan con el caracter *<*
    * Puede haber lineas en blanco.
    * Cada linea termina con un punto que hay que descartar.
    * Un sujeto puede aparecer en varias lineas con distintas
      relacion/predicado en *mappingbased_properties_en.nt*
    * Un sujeto puede tener varios tipos en *instance_types_en.nt*.
    * La sintaxis de tipo en el archivo instance_types_en.nt no hay
      que tenerla en cuenta en el algoritmo.

El programa debe contar la cantidad de propiedades
(lineas en el archivo *mappingbased_properties_en.nt*)
por tipo del sujeto utilizando solo la API de datasets.

A continuación se presentan los tipos que deben ser usados:

case class Propiedad (sujeto: String, verbo: String, predicado: String)
case class Tipo(entidad: String, tipo: String)
case class Tipos(entidad: String, tipos: Array[String])
case class PropTipo (sujeto: String, tipo: String, verbo: String, predicado: String)

// cargar SparkSparkSession
val inPropRaw : Dataset[String] = spark.read.text("/doc/mappingbased_properties_en.nt").as[String]
*/

case class Propiedad(sujeto: String, verbo: String, predicado: String)
case class Tipo(entidad: String, tipo: String)
case class Tipos(entidad: String, tipos: Array[String])
case class PropTipo(sujeto: String, tipo: String, verbo: String, predicado: String)

// cargar SparkSparkSession
val FULL_PATH = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/zeppelin-0.7.3-bin-all/doc/"
val inPropRaw : Dataset[String] = spark.read.text(FULL_PATH + "mappingbased_properties_en.nt").as[String]
