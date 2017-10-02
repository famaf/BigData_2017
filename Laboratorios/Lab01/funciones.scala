import org.apache.spark.rdd.RDD
import com.github.nscala_time.time.Imports._
// import scala.io.Source

// Hay lineas que no tienen 6 columnas
// para catchear excepciones usar la libreria: scala.until.try

// Lista de paths de los archivos de Sci-Hub
val PATH_1 = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/Laboratorios/Lab01/scihub_data/dec2015.tab"
val PATH_2 = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/Laboratorios/Lab01/scihub_data/feb2016.tab"
val PATH_3 = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/Laboratorios/Lab01/scihub_data/jan2016.tab"
val PATH_4 = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/Laboratorios/Lab01/scihub_data/nov2015.tab"
val PATH_5 = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/Laboratorios/Lab01/scihub_data/oct2015.tab"
val PATH_6 = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/Laboratorios/Lab01/scihub_data/sep2015.tab"

val PATH_LIST = List(PATH_1, PATH_2, PATH_3, PATH_4, PATH_5, PATH_6)
/*****************************************************************************/


/** Carga un archivo en memoria y retorna un RDD.
  *  
  *  @param     file                Path al archivo
  *  @return    RDD[Array[String]]  Cada Array[String] tiene 6 elementos:
  *                                 date, doi, ip_code, country, city, coords
 */
def loadFile(file : String) : RDD[Array[String]] = {
    val rdd_lines = sc.textFile(file)  // type: RDD[String]
    // val rdd_file = rdd_lines.map(l => l.split("\t"))
    val rdd_file = rdd_lines.map(_.split("\t"))

    return rdd_file
}
/*****************************************************************************/


/** Carga una lista de archivos en memoria y retorna un unico
  * RDD[Array[String]] con el contenido de todos los archivos.
  *  
  *  @param     files               Lista de paths
  *  @return    RDD[Array[String]]  Cada Array[String] tiene 6 elementos:
  *                                 date, doi, ip_code, country, city, coords
 */
def loadDataset(files: List[String]) : RDD[Array[String]] = {
    val rdd_empty : RDD[Array[String]] = sc.emptyRDD[Array[String]]
    val rdd_file = files.map(loadFile).fold(rdd_empty)(_.union(_))
    // val rdd_file = files.map(loadFile(_)).fold(rdd_empty)(_.union(_))

    return rdd_file
}
/*****************************************************************************/


/** Transforma un RDD[Array(String)] en un RDD[(DateTime,Int)].
  *  
  *  @param     raw                 Dataset "crudo" de sci-hub.
  *  @return    RDD[(LocalDate,Int)]
 */
// def toDateTuple(raw: RDD[Array[String]]) : RDD[(LocalDate, Int)] = {
//     val rdd_date_time = raw.map(x => x(0))
//     val rdd_date = rdd_date_time.map(x => x.split(" ")(0).split("-"))

//     def transform_date(date : Array[String]) : LocalDate = {
//         try {
//             var year = Integer.parseInt(date(0))
//             var month = Integer.parseInt(date(1))
//             var day = Integer.parseInt(date(2))

//             return (new LocalDate(year, month, day))
//         }
//         catch {
//             case e: Exception =>
//                 println("Error")
//                 return (new LocalDate(2002, 4, 4))
//         }
//     }

//     val tuple_rdd = rdd_date.map(x => (transform_date(x), 1))

//     return tuple_rdd
// }
/*****************************************************************************/


// def aggregateByDay(data : RDD[(LocalDate, Int)]) = {

// }
/*****************************************************************************/


// def aggregateByMonth(data : RDD[(LocalDate, Int)]) = {

// }
/*****************************************************************************/


// def aggregateByWeekDay(data : RDD[(LocalDate, Int)]) = {

// }
/*****************************************************************************/
