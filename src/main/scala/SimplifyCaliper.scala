import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object MarshallableImplicits {   // From https://www.programcreek.com/scala/com.fasterxml.jackson.module.scala.DefaultScalaModule

  implicit class Unmarshallable(unMarshallMe: String) {
    def toMap: Map[String,Any] = JsonUtil.toMap(unMarshallMe)
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



case class Event(id: String, actor: String, event_type: String, action: String,
  event_object: String, event_time: String, target: String,
  generated: String, edApp: String, referrer: String,
  group: String, membership: String, session: String,
  federatedSession: String, extensions: Map[String,Object])

object SimplifyCaliper extends App {

//  val spark = SparkSession.builder
//    .master("local[*]")
//    .appName("Spark Word Count")
//    .getOrCreate()

  // Modifying to get sc from databricks
  val sparkContext = SparkContext.getOrCreate()
  val spark = SparkSession.builder().getOrCreate()

  //  spark.sparkContext.hadoopConfiguration.set("fs.azure.account.auth.type.acctname.dfs.core.windows.net", "OAuth")
  //  spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth.provider.type.acctname.dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  //  spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.id.acctname.dfs.core.windows.net", "<client-id>")
  //  spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.secret.acctname.dfs.core.windows.net", dbutils.secrets.get(scope = "<scope-name>", key = "<key-name-for-service-credential-for-service-credential>"))
  //  spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.endpoint.acctname.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")


  //
  val sc = spark.sparkContext
 val rawCaliper = spark.sparkContext.textFile("abfss://data@saildatalake.dfs.core.windows.net/testcourse/f16-15319/raw/caliper/")



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
        case submap : Map[String,Object] => theSimplerMap(k) = submap.toString ;  List()
        case null => List()
        case somethingElse => theSimplerMap(k) = somethingElse.toString;  List()
      }}

    keys.flatMap(separator).toList :+ theSimplerMap.toMap
  }

  def decorate_and_split_caliper(theMap : Map[String, Object]) : List[Map[String,String]] = {
      val records = flatten(theMap("data").asInstanceOf[List[Map[String, Object]]](0))

      records.map( rec => rec + ("sensor" -> theMap("sensor").toString))
  }

  val test1 = """{"id": 42, "name": "item42", "subthing": {"id": "42b", "subname": "subthing42"}} """
  println(flatten(JsonUtil.toMap[Object](test1)))

  import spark.implicits._

  val result = rawCaliper.mapPartitions(records => {
    records.map(JsonUtil.toMap[Object]).flatMap(
      decorate_and_split_caliper
    )
    }, true)

  print(rawCaliper.getClass.getSimpleName())

  val events = result.mapPartitions(records => {
    records.filter( q => (q contains "type") && q("type") == "Person")
  })


  val columns=events.take(1).flatMap(a=>a.keys)

  val resultantDF=events.map{value=>
    val list=value.values.toList
    (list(0),list(1), list(2), list(3))
  }.toDF(columns:_*)

  resultantDF.printSchema()
  resultantDF.show(2, false)

  //create properties object

//  val prop = new java.util.Properties
//  prop.setProperty("driver", "com.mysql.jdbc.Driver")
//  prop.setProperty("user", "XXXXXXXXXXX")
//  prop.setProperty("user", "XXXXX")
//  prop.setProperty("password", "XXXXXXXX")

  //jdbc mysql url - destination database is named "fall2019" - local, "test" - remote
//  val url = "jdbc:mysql://localhost:3306/fall2019"
//  val url = "jdbc:mysql://localhost:3306/fall2019?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
//  val url = "jdbc:mysql://XXXXXXXXXXXX/test?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"

  //destination database table
//  val table = "localtransfer"

  //write data from spark dataframe to database
//  resultantDF.write.mode("append").jdbc(url, table, prop)
  println( "Ready!");
  //result.foreach(println);


  println("Done!");
}
