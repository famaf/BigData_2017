// ############################################################################
// ############################## PRIMERA PARTE ###############################
// ############################################################################
import org.apache.spark.rdd.RDD
import com.github.nscala_time.time.Imports._

// Hay lineas que no tienen 6 columnas
// para catchear excepciones usar la libreria: scala.until.try

//***************************************************************************//
//**************** Lista de paths de los archivos de Sci-Hub ****************//
//***************************************************************************//
val PATH_1 = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/Laboratorios/Lab01/scihub_data/dec2015.tab"
val PATH_2 = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/Laboratorios/Lab01/scihub_data/feb2016.tab"
val PATH_3 = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/Laboratorios/Lab01/scihub_data/jan2016.tab"
val PATH_4 = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/Laboratorios/Lab01/scihub_data/nov2015.tab"
val PATH_5 = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/Laboratorios/Lab01/scihub_data/oct2015.tab"
val PATH_6 = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/Laboratorios/Lab01/scihub_data/sep2015.tab"
val PATH_LIST = List(PATH_1, PATH_2, PATH_3, PATH_4, PATH_5, PATH_6)
//***************************************************************************//


/*****************************************************************************/

/** Carga un archivo en memoria y retorna un RDD.
  *  
  *  @param     file                Path al archivo
  *  @return    RDD[Array[String]]  Cada Array[String] tiene 6 elementos:
  *                                 date, doi, ip_code, country, city, coords
 */
def loadFile(file : String) : RDD[Array[String]] = {
    // val rdd_file = rdd_lines.map(l => l.split("\t"))
    val rdd_lines = sc.textFile(file)  // type: RDD[String]
    val rdd_file = rdd_lines.map(_.split("\t"))  // split convierte a Array
    // val rdd_ret = rdd_file.filter(array => array.length == 6)
    val rdd_ret = rdd_file.filter(_.length == 6) // La linea esta bien formada (6 columnas)

    return rdd_ret
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
def toDateTuple(raw: RDD[Array[String]]) : RDD[(LocalDate, Int)] = {
    val rdd_date_time = raw.map(x => x(0))
    val rdd_date = rdd_date_time.map(x => x.split(" ")(0))
    val rdd_year_month_day = rdd_date.map(x => x.split("-"))

    def transform_date(date : Array[String]) : LocalDate = {
        try {
            var year = Integer.parseInt(date(0))
            var month = Integer.parseInt(date(1))
            var day = Integer.parseInt(date(2))

            val local_date = new LocalDate(year, month, day)

            return (local_date)
        }
        catch {
            case e: Exception =>
                println("### Error ###")
                val local_date = new LocalDate(3000, 12, 31)  // Preguntar que hacer en este caso

                return (local_date)
        }
    }

    val rdd_tuple = rdd_year_month_day.map(x => (transform_date(x), 1))

    return rdd_tuple
}
/*****************************************************************************/

def aggregateByDay(data : RDD[(LocalDate, Int)]) : RDD[(LocalDate, Int)] = {
    val rdd_ret = data.reduceByKey(_ + _)

    return rdd_ret
}
/*****************************************************************************/

def aggregateByMonth(data : RDD[(LocalDate, Int)]) : RDD[(LocalDate, Int)] = {
    val rdd_months = data.map(x => (new LocalDate(x._1.getValue(0),
                                                  x._1.getValue(1), 1),
                                    1)
                             )
    val rdd_ret = rdd_months.reduceByKey(_ + _)

    return rdd_ret
}
/*****************************************************************************/

def aggregateByWeekDay(data : RDD[(LocalDate, Int)]) : RDD[(Int, Int)] = {
    val rdd_days = data.map(x => (x._1.getDayOfWeek(), 1))
    val rdd_ret = rdd_days.reduceByKey(_ + _)

    return rdd_ret
}
/*****************************************************************************/

// ################################# Ejecucion ################################
// val rdd_dataset = loadDataset(PATH_LIST)
val rdd_dataset = loadFile(PATH_1)
val rdd_datetuple = toDateTuple(rdd_dataset)
val rdd_day = aggregateByDay(rdd_datetuple)
val rdd_month = aggregateByMonth(rdd_datetuple)
val rdd_weekday = aggregateByWeekDay(rdd_datetuple)

// Esto es para Zeppelin (Descargas x Dia)
// println("%table Date\tDownloads\n" + table1)
// rdd_day.collect.foreach{case (w,c) => println("\"" + w + "\"" + "\t" + c)}
def save_descargasxdia(data: RDD[(LocalDate, Int)]) = {
    import java.io._

    val pw = new PrintWriter(new File("descargas_x_dia.txt"))

    pw.write("Descargas x Dia\n===============\n")
    pw.write("Date\t\t\tDownloads\n")
    data.collect.foreach{case (w,c) => pw.write("\"" + w + "\"" + "\t" + c + "\n")}
    pw.close
    println("\n##### Se creo el archivo 'descargas_x_dia.txt' #####\n")
}

// Esto es para Zeppelin (Descargas x Mes)
// println("%table Month\tDownloads\n" + table2)
// rdd_month.collect.foreach{case (w,c) => println("\"" + w.getValue(0) + "-" + w.getValue(1) + "\"" + "\t" + c)}
def save_descargasxmes(data: RDD[(LocalDate, Int)]) = {
    import java.io._

    val pw = new PrintWriter(new File("descargas_x_mes.txt"))

    pw.write("Descargas x Mes\n===============\n")
    pw.write("Month\t\tDownloads\n")
    data.collect.foreach{case (w,c) => pw.write("\"" + w.getValue(0) + "-" + w.getValue(1) + "\"" + "\t" + c + "\n")}
    pw.close
    println("\n##### Se creo el archivo 'descargas_x_mes.txt' #####\n")
}

// Esto es para Zeppelin (escargas x Dia de la semana)
// def dias(s: String) : String={
//     s match {
//       case "1"  => return("lunes")
//       case "2"  => return("martes")
//       case "3"  => return("miercoles")
//       case "4"  => return("jueves")
//       case "5"  => return("viernes")
//       case "6"  => return("sabado")
//       case "7"  => return("domingo")
//     }
// }
// println("%table Day\tDownloads\n" + table3)
// rdd_weekday.takeOrdered(7)(Ordering[Int].on (_._1.toInt)).map(x => (dias(x._1.toString),
//                                                                     x._2)
//                                                                    ).foreach{case (w,c) => println("\"" + w + "\"" + "\t" + c)}
def save_descargasxdiasemanal(data: RDD[(Int, Int)]) = {
    import java.io._

    def dias(s: String) : String = {
        s match {
          case "1"  => return("lunes")
          case "2"  => return("martes")
          case "3"  => return("miercoles")
          case "4"  => return("jueves")
          case "5"  => return("viernes")
          case "6"  => return("sabado")
          case "7"  => return("domingo")
        }
    }

    val pw = new PrintWriter(new File("descargas_x_dia_semana.txt"))

    pw.write("Descargas x Dia de la semana\n============================\n")
    pw.write("Day\t\tDownloads\n")
    data.takeOrdered(7)(Ordering[Int].on (_._1.toInt)).map(x => (dias(x._1.toString),
                                                                 x._2)
                                                          ).foreach{case (w,c) => pw.write("\"" + w + "\"" + "\t" + c + "\n")}
    pw.close
    println("\n##### Se creo el archivo 'descargas_x_dia_semana.txt' #####\n")
}

// save_descargasxdia(rdd_day)
// save_descargasxmes(rdd_month)
// save_descargasxdiasemanal(rdd_weekday)

// ############################################################################
// ############################## SEGUNDA PARTE ###############################
// ############################################################################
import au.com.bytecode.opencsv.CSVReader
import java.io.StringReader
import scalaj.http._
import scala.util.parsing.json._


/** Carga un archivo contiene el mapeo prefix -> publisher..foreach(println)
*  
*  @param   file                    Path al archivo.
*  @return  RDD[(String,String)]    
*/
def loadPrefixMapping(file: String) : RDD[(String, String)] = {
    val lines = sc.textFile(file)

    // Funcion que devuelve false si la linea, antes de la primer coma,
    // no es un numero
    def removeHeader(line: String) : Boolean = {
        val first = line.split(",")(0)
        try {
           var a = Integer.parseInt(first)
           return (true)
        }
        catch {
            case e: Exception =>
                return (false)
        }
    }

    val linesWithOutHeader = lines.filter(removeHeader)
    val rdd_arr = linesWithOutHeader.map(x => new CSVReader(new StringReader(x),
                                                           ',',
                                                           '"',
                                                           0).readAll().get(0))

    val resultado = rdd_arr.map(arr => (arr(2), arr(1)))
    return (resultado)
}

def IDtoNAME(s: String, rdd: RDD[(String, String)]) : String = {
    return rdd.lookup(s)(0)
}

def filtrarErroneo(s: String) : Boolean = {
    val arr = s.split("\\.")
    val l = arr.length
    return l == 2
}

def aggregateByPrefix(rdd: RDD[Array[String]], rddMapeo: RDD[(String, String)]) : RDD[(String, Int)] = {
    val rdd_ids1 = rdd.map(x => (x(1).split("/")(0), 1))
    val rdd_ids = rdd_ids1.filter(x => filtrarErroneo(x._1))
    // rdd_ids.take(10).foreach(println)
    // val rdd_prefix = rdd_ids.map(x => (IDtoNAME(x._1, rddMapeo), 1))
    val rdd_ret1 = rdd_ids.reduceByKey(_ + _)

    val rdd_ret = rdd_ret1.join(rddMapeo.distinct())
    val h = rdd_ret.map(x => (x._2._2, x._2._1))
    // val prueba = rdd_ret.collect.map(x =>(IDtoNAME(x._1, rddMapeo), 1))

    return(h)
}

def aggregateByCountry(rdd: RDD[Array[String]]) : RDD[(String, Int)] = {
    val rdd_country = rdd.map(x => (x(3), 1))
    val rdd_ret = rdd_country.reduceByKey(_ + _)

    return rdd_ret
}

def aggregateByDoi(rdd: RDD[Array[String]]) : RDD[(String, Int)] = {
    val rdd_doi = rdd.map(x => (x(1), 1))
    val rdd_ret = rdd_doi.reduceByKey(_ + _)

    return rdd_ret
}

/** Retorna la metadata asociada a un DOI
  *
  *  @param     doi               DOI
  *  @return    ...
  */
def getMetadata(doi: String) : Array[String] = {
    try {
        val url = "https://data.crossref.org/"
        val header_accept = "application/rdf+xml;q=0.5, application/vnd.citationstyles.csl+json;q=1.0"
        // val response = Http(url ++ doi).header("Accept", header_accept).asString.body.toString
        val response = Http("https://data.crossref.org/" ++ doi).header("Accept", "application/rdf+xml;q=0.5, application/vnd.citationstyles.csl+json;q=1.0").asString.body.toString
        val a = JSON.parseFull(response).get.asInstanceOf[Map[String, List[Map[String, String]]]]

        var title = ""
        var year = ""
        var publisher = ""
        var book = ""
        var subject = ""
        var type1 = ""

        try{
            title = a("title").asInstanceOf[String]
        }
        catch {
            case e: Exception =>
            title = "No Metadatos Disponibles"
        }

        try {
            year = a.asInstanceOf[Map[String, Map[String, List[List[Double]]]]]("issued")("date-parts")(0)(0).toString.split("\\.")(0)
        }
        catch {
            case e: Exception =>
            year = "No Metadatos Disponibles"
        }

        try {
            type1 = a("type").asInstanceOf[String]
        }
        catch {
            case e: Exception =>
            type1 = "No Metadatos Disponibles"
        }

        try {
            publisher = a("publisher").asInstanceOf[String]
        }
        catch {
            case e: Exception =>
            publisher = "No Metadatos Disponibles"
        }

        try {
            book = a("container-title").asInstanceOf[String]
        }
        catch {
            case e:Exception =>
            book = "No Metadatos Disponibles"
        }

        try {
            subject = a("subject").asInstanceOf[List[String]](0)
        }
        catch {
            case e: Exception =>
            subject = "No Metadatos Disponibles"
        }

        return (Array(title, year, type1, publisher, book, subject))

    }
    catch {
        case e: Exception =>
        println("### Error al descargar metadatos. El servidor no responde ###")
        return (Array("error", "error", "error", "error", "error", "error"))
        // El error es causado por un timeout al descargar los metadatos. no los descarga
    }
}

def completarMetadatos(par: (String, Int)) : (String, String, String, String, String, String, String, String) = {
    val doi = par._1
    val res = getMetadata(doi)

    return ((doi, res(0), res(1), res(2), res(3), res(4), res(5), par._2.toString))
}

// getMetadata("10.1126/science.aad0135")
// val url = "https://data.crossref.org/"
// val url2 = "10.1126/science.aad0135"
// val header_accept = "application/rdf+xml;q=0.5, application/vnd.citationstyles.csl+json;q=1.0"
// val response = Http(url ++ url2).header("Accept", header_accept).asString.body.toString
// println("  ")
// println("  ")
// println("  ")
// val a = JSON.parseFull(response).get.asInstanceOf[Map[String,List[Map[String,String]]]]
// println("  ")
// println("  ")
// println("  ")
// val y = a.asInstanceOf[Map[String,Map[String,List[List[Double]]]]]("issued")("date-parts")(0)(0).toString.split("\\.")(0)


// ################################# Ejecucion ################################
val PATH_PREFIXES = "/home/mario/Documentos/FaMAF/Optativas/BigData_2017/Laboratorios/Lab01/publisher_DOI_prefixes.csv"
val pares = loadPrefixMapping(PATH_PREFIXES)

val rdd_prefix = aggregateByPrefix(rdd_dataset, pares).takeOrdered(10)(Ordering[Int].reverse.on (_._2))
val rdd_country = aggregateByCountry(rdd_dataset).takeOrdered(10)(Ordering[Int].reverse.on (_._2))
val rdd_doi = aggregateByDoi(rdd_dataset).takeOrdered(10)(Ordering[Int].reverse.on (_._2))
// val rdd_doi_plus_metadatos = rdd_doi.map(x => completarMetadatos(x))

///////////////////////////////////////////////////////////////////////
// PROBLEMA CUANDO QUIERE OBTENER LOS DATOS DEL SERVIDOR ==> rdd_doi //
///////////////////////////////////////////////////////////////////////

// Esto es para Zeppelin (Top 10 - Editoriales)
// println("%table Publisher\tDownloads\n" + table4)
// rdd_prefix.foreach{case (w,c) => println("\"" + w + "\"" + "\t" + c)}
def save_top10editoriales(data: Array[(String, Int)]) = {
    import java.io._

    val pw = new PrintWriter(new File("top_10_editoriales.txt"))

    pw.write("Top 10 - Editoriales\n====================\n")
    pw.write("Publisher\tDownloads\n")
    data.foreach{case (w,c) => pw.write("\"" + w + "\"" + "\t" + c + "\n")}
    pw.close
    println("\n##### Se creo el archivo 'top_10_editoriales.txt' #####\n")
}


// Esto es para Zeppelin (Top 10 - Paises)
// println("%table Publisher\tDownloads\n" + table4)
// rdd_country.foreach{case (w,c) => println("\"" + w + "\"" + "\t" + c)}
def save_top10paises(data: Array[(String, Int)]) = {
    import java.io._

    val pw = new PrintWriter(new File("top_10_paises.txt"))

    pw.write("Top 10 - Paises\n===============\n")
    pw.write("Publisher\tDownloads\n")
    data.foreach{case (w,c) => pw.write("\"" + w + "\"" + "\t" + c + "\n")}
    pw.close
    println("\n##### Se creo el archivo 'top_10_paises.txt' #####\n")
}


// Esto es para Zeppelin (Top 10 - Articulos)
// println("%table DOI\tTitle\tYear\tType\tPublisher\tBook\tSubject\tDownloads\n" + table5)
// rdd_doi_plus_metadatos.foreach{case (d,t,y,ti,p,b,s,dow) => println(d + "\t" + t + "\t" + y + "\t" + ti + "\t" + p + "\t" + b + "\t" + s + "\t" + dow)}
def save_top10articulos(data: Array[(String, String, String, String, String, String, String, String)]) = {
    import java.io._

    val pw = new PrintWriter(new File("top_10_articulos.txt"))

    pw.write("Top 10 - Articulos\n==================\n")
    pw.write("DOI\tTitle\tYear\tType\tPublisher\tBook\tSubject\tDownloads\n")
    data.foreach{case (d,t,y,ti,p,b,s,dow) => println(d + "\t" +
                                                      t + "\t" +
                                                      y + "\t" +
                                                      ti + "\t" +
                                                      p + "\t" +
                                                      b + "\t" +
                                                      s + "\t" +
                                                      dow)}
    pw.close
    println("\n##### Se creo el archivo 'top_10_articulos.txt' #####\n")
}


save_top10editoriales(rdd_prefix)
save_top10paises(rdd_country)
// save_top10articulos(rdd_doi_plus_metadatos)
