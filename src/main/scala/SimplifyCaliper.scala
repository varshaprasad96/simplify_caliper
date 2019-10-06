import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, StructType, TimestampType}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.SparkContext

object MarshallableImplicits {   // From https://www.programcreek.com/scala/com.fasterxml.jackson.module.scala.DefaultScalaModule

  implicit class Unmarshallable(unMarshallMe: String) {
    def toMap: Map[String,Any] = JsonUtil.toMap(unMarshallMe)
    //def toMapOf[V]()(implicit m: Manifest[V]): Map[String,V] = JsonUtil.toMapOf[V](unMarshallMe)
    def fromJson[T]()(implicit m: Manifest[T]): T =  JsonUtil.fromJson[T](unMarshallMe)
  }

  implicit class Marshallable[T](marshallMe: T) {
    def toJson: String = JsonUtil.toJson(marshallMe)
  }
}

//https://coderwall.com/p/o--apg/easy-json-un-marshalling-in-scala-with-jackson

object JsonUtil {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def toMap[V](json:String)(implicit m: Manifest[V]) = fromJson[Map[String,V]](json)

  def fromJson[T](json: String)(implicit m : Manifest[T]): T = {
    mapper.readValue[T](json)
  }
}


object SimplifyCaliper extends App {

  val sparkSession = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  //val sparkContext = SparkContext.getOrCreate()
  //val sparkSession = SparkSession.builder().getOrCreate()

  val sc = sparkSession.sparkContext
  val sparkContext = sc
  //val rawCaliper = spark.read.text("/Users/cbogart/dropbox/research/sail/spark/caliper/*")
    //.select(input_file_name, $"value")
    //.as[(String, String)] // Optionally convert to Dataset
    //.rdd





  def object2id(obj : Object): String = obj match {
    case None => ""
    case a : String => a
    case subobject : Map[String,Object] =>
      if (subobject contains "id") { subobject ("id").asInstanceOf[String] } else { "" }
    case _ => ""
  }

  def flatten(theMap : Map[String,Object]) : List[Map[String,String]] = {
    val theSimplerMap = new scala.collection.mutable.HashMap[String,String]()

    val keys = theMap.keysIterator
    def separator(k : String) : Seq[Map[String,String]] = {
      theMap(k) match {
        case value : String =>  theSimplerMap(k) = value ;  List()
        case submap : Map[String,Object] if submap contains "id" =>  theSimplerMap(k) = submap("id").toString; flatten(submap)
        case submap : Map[String,Object] => theSimplerMap(k) = JsonUtil.toJson(submap) ; List()
        case null => List()
        case somethingElse => theSimplerMap(k) = JsonUtil.toJson(somethingElse);  List()
      }}

    keys.flatMap(separator).toList :+ theSimplerMap.toMap
  }

  def decorate_and_split_caliper(theMap : Map[String, Object]) : List[Map[String,String]] = {
      val records = flatten(theMap("data").asInstanceOf[List[Map[String, Object]]](0))

      records.map( rec => rec + ("sensor" -> theMap("sensor").toString))
  }

  def structField(columnName: String) : StructField = {
    if (columnName == "eventTime") return StructField(columnName, StringType, nullable = true)
    if (columnName startsWith "date") return StructField(columnName, StringType, nullable = true)
    if (columnName == "id") return StructField(columnName, StringType, nullable = true)
    return StructField(columnName, StringType, nullable = true)
  }

  def map_list_to_schema(maplist: Iterable[Map[String,String]]): StructType = {
    val all_cols = maplist.map(_.keySet).reduce( (set1 : Set[String], set2: Set[String]) => set1.union(set2) )
    StructType(all_cols.toArray.map( colname  => structField(colname)))
  }

  def map2row(theMap: Map[String,String], schema: StructType): Row = {
    Row.fromSeq(schema.fieldNames.map( v =>
      if (theMap.contains(v)) theMap(v) else null ))
  }


  val test1 = """{"id": 42, "name": "item42", "subthing": {"id": "42b", "subname": "subthing42"}}"""
  println(flatten(JsonUtil.toMap[Object](test1)))

  /*val result = rawCaliper.zip(input_file_name()).mapPartitions(records => {
    records.map(JsonUtil.toMap[Object]).flatMap(
      decorate_and_split_caliper
    )
    }, true)
*/

  def convertDir(directory : String) {
    val courseSlug = directory.split("/").last.replace("-","_")
    val testFlag = directory.contains("testcourse")
    val rawCaliper = sc.textFile(directory + "/raw/caliper/*")
    val dbname = (if (testFlag) "test_" else "") + courseSlug
    println("Writing to database " + dbname)
    //val rawWholeCaliper = sc.wholeTextFiles("/Users/cbogart/downloads/testcourse/*/raw/caliper/*")


    val result = rawCaliper.mapPartitions(records => {
      records.map(JsonUtil.toMap[Object]).flatMap(
        decorate_and_split_caliper
      )
    }, preservesPartitioning = true)

    import sparkSession.implicits._

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "")
    prop.setProperty("password", "")
    val url = "jdbc:mysql://testdatapipeline.mysql.database.azure.com:3306/" + dbname + "_t?createDatabaseIfNotExist=true&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"



    /*def accum_duplicates(df : DataFrame): DataFrame = {
    val newrows = Map[String,Row]
    df.foreach( row =>
      newrows(row["id"])
    )
    newrows
  }*/

    result.groupBy(r => if (r("type").endsWith("Event")) "Event" else r("type")).foreach {
      case (eventType: String, items: Seq[Map[String, String]]) => {
        val schema = map_list_to_schema(items);
        val rdd = sc.parallelize(items.map(map2row(_, schema)))
        val df = sparkSession.createDataFrame(rdd, schema)
        //val uniquedf = accum_duplicates(df)
        println("Table for " + eventType + " has " + df.collect().length.toString + " records")
        df.write.mode("append").jdbc(url, if (eventType.endsWith("Event")) "Event" else eventType, prop)
      }
    }

  }

  convertDir("/Users/cbogart/downloads/testcourse/f19-15319")
  convertDir("/Users/cbogart/downloads/not_within_course")
}
