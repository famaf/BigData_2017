/*
1. Dado el dataset *userid_profile.tsv* el cual tiene el siguiente formato:

id \t gender ('m'|'f'|empty) \t age (int|empty) \t country (str|empty) \t registered (date|empty)

    a. Si usa Spark 2.0: Cree un *DataSet* a partir del archivo usando Reflection.
    b. Si usa Spark 1.6.x: Cree un *DataFrame* a partir del archivo
       especificando explícitamente el esquema del *DataFrame*.
    c. Guarde el Dataset (o el DataFrame) en formato parquet

2. Usando el dataset en formato parquet creado en el punto anterior,
   compute las siguientes métricas usando SQL.

    a. Cantidad de usuarios por país.
    b. Cantidad de usuarios por país desagregado por sexo.
    c. Edad promedio según el genero del usuario.
    d. Cantidad de usuarios por fecha de registración.
       Genere un gráfico para esta métrica.

3. Ahora calcule las mismas métricas esta vez de forma programatica
   (i.e.: usando los métodos provistos por el dataset/dataframe).
*/


/* Sanitizacion del dataset y creacion del archivo parquet */
import scala.util.Try
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.SaveMode

val base_dir = "/Users/ezequielorbe/Teaching/Spark2016/datasets/last-fm/lastfm-dataset-1K"
val source_file = s"${base_dir}/userid-profile.tsv"
val target_file = s"${base_dir}/users_profiles.parquet"

case class User(id: String, gender: String, age: Int,
                country: String, registered: java.sql.Timestamp)

def createUser(att: Array[String]): User = {
    val signup = Try({
            new java.sql.Timestamp(DateTimeFormat.forPattern("MMMM d, yyyy")
                .parseDateTime(att(4))
                .toDate.getTime())
        }) getOrElse  new java.sql.Timestamp(0)

    val age = Try(att(2).trim.toInt) getOrElse 0

    User(att(0), att(1), age, att(3), signup)
}

// Metodo artesanal: levantando un rdd, limpiandolo y luego creando el dataset.
val ds = sc.textFile(source_file)
    .map(x => x.split("\t"))
    .filter(x => x.length == 5 && x(0) != "id")
    .map(x => createUser(x))
    .toDS()

ds.write
    .mode(SaveMode.Overwrite)
    .parquet(target_file)


/* Levantando el archivo parquet y registrandolo como tabla */
val ds1 = spark.read.parquet(target_file).as[User]
ds1.createOrReplaceTempView("users")
