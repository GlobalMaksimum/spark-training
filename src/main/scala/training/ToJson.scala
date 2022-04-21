package training

import org.apache.spark.sql.SparkSession

object ToJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("IntroRDD").master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.sqlContext.implicits._

    val movies = sc.textFile(s"${ParsingUtils.dPath}/movies.csv").filter(l => l!="movieId,title,genres").map(ParsingUtils.parseMovie)
    val ratings = sc.textFile(s"${ParsingUtils.dPath}/ratings.csv").filter(_!="userId,movieId,rating,timestamp").map(ParsingUtils.parseRating)
    val tags = sc.textFile(s"${ParsingUtils.dPath}/tags.csv").filter(_!="userId,movieId,tag,timestamp").map(ParsingUtils.parseTag)
    val links = sc.textFile(s"${ParsingUtils.dPath}/links.csv").filter(_!="movieId,imdbId,tmdbId").map(ParsingUtils.parseLink)

    val moviesDF = movies.toDF()
    val ratingsDF = ratings.toDF()
    val tagsDF = tags.toDF()
    val linksDF = links.toDF()

    //tagsDF.printSchema()

    moviesDF.coalesce(1).write.json(s"${ParsingUtils.dPath}/movies.json")
    ratingsDF.coalesce(1).write.json(s"${ParsingUtils.dPath}/ratings.json")
    tagsDF.coalesce(1).write.json(s"${ParsingUtils.dPath}/tags.json")
    linksDF.coalesce(1).write.json(s"${ParsingUtils.dPath}/links.json")
  }
}
