import org.apache.spark.sql.SparkSession

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.functions.input_file_name

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

case class Event(id: String, actor: String, event_type: String, action: String,
  event_object: String, event_time: String, target: String,
  generated: String, edApp: String, referrer: String,
  group: String, membership: String, session: String,
  federatedSession: String, extensions: Map[String,Object])

object SimplifyCaliper extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  val sc = spark.sparkContext

  //val rawCaliper = spark.read.text("/Users/cbogart/dropbox/research/sail/spark/caliper/*")
    //.select(input_file_name, $"value")
    //.as[(String, String)] // Optionally convert to Dataset
    //.rdd


  val rawCaliper = sc.textFile("/Users/cbogart/dropbox/research/sail/spark/caliper/*")


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

  val test1 = """{"id": 42, "name": "item42", "subthing": {"id": "42b", "subname": "subthing42"}}"""
  println(flatten(JsonUtil.toMap[Object](test1)))

  val result = rawCaliper.zip(input_file_name()).mapPartitions(records => {
    records.map(JsonUtil.toMap[Object]).flatMap(
      decorate_and_split_caliper
    )
    }, true)

  val events = result.mapPartitions(records => {
    records.filter( q => (q contains "type") && q("type") == "Person"  )
  })

  println( "Ready!");

  events.foreach(println)

  println("Done!");
}
