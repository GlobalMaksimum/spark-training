package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType,IntegerType}

object IntroDataFrames {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().master("local[*]").appName("IntroRDD").getOrCreate()

    val df = spark.range(100000000).toDF("number")

    println(df.count())
    // TODO fix paths
    val movies = spark.read.json(s"/data/movies.json")
    val tagsInferred = spark.read.json(s"/data/tags.json")
    spark.read.format("json").options(Map("inferSchema"->"true")).load("/data/tags.json")

    //.option("mode","FAILFAST")
    //DROPMALFORMED
    //PERMISIVE

    val tagsSchema = StructType(Array(StructField("movieId",LongType,true), StructField("tag",StringType,true), StructField("ts",TimestampType,true), StructField("userId",IntegerType,true)))
    val tags = spark.read.format("json").schema(tagsSchema).load("/data/tags.json")

    import org.apache.spark.sql.functions._
    import spark.implicits._


    import spark.implicits._
    import org.apache.spark.sql.types._

    movies.select(col("id"),col("year"),$"id",expr("movieId")).show(2)

    movies.withColumn("myNumovies.mber",lit("dsadsa")).show(2)
  }
}
