package training

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

object IntroRDD {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().master("local[*]").appName("IntroRDD").getOrCreate()

    val sc = spark.sparkContext

    val basicRDD: RDD[Int] = sc.parallelize(1 to 10000)
    val line =
      "To create an RDD from a collection, you will need to use the parallelize method on a SparkContext (within a SparkSession). This turns a single node collection into a parallel collection. When creating this parallel collection, you can also explicitly state the number of partitions into which you would like to distribute this array."
    val rddString: RDD[String] = sc.parallelize(line.split(' '))

    val ratingsLines = sc.textFile("environment/data/small/ratings.csv",4)

    val ratings: RDD[Rating] = ratingsLines.filter(line => line != "userId,movieId,rating,timestamp").map { line =>
      line.split(',').toList match {
        case userId :: movieId :: rating :: timestamp :: Nil =>
          Rating(
            userId.toInt,
            movieId.toInt,
            rating.toDouble,
            new Timestamp(timestamp.toLong * 1000)
          )
      }
    }

    // monadic flatMapten farkli F[T]:flatMap  T => F[T]  concatMap
    // val ratingsF =  ratings.flatMap(rating => List(rating,rating))
    // RDD immutable transformation,action
    // transformation => RDD => RDD lazy
    // action => RDD => T
    // r1 => r2 => r3 => r4 => r5 all transformations stored as lazy
    // t1 => t2 => t3 => t4 => t5 => a


    // Keyed RDD
    val keyedRatings: RDD[(Int, Rating)] = ratings.map(rating => (rating.movieId, rating))
    val groupedRatings: RDD[(Int, Iterable[Rating])] = keyedRatings.groupByKey()

    val tupled: RDD[(Int, (Double, Int))] = groupedRatings.mapValues(v => v.foldLeft((0.0d, 0)) { case (total, e) => (total._1 + e.rating, total._2 + 1) })

    val keyedRatings2: RDD[(Int, Int)] = ratings.map(rating => (rating.movieId, 1))
    val reducedRatings2  = keyedRatings2.reduceByKey(_+_)

    val bycombine = keyedRatings.combineByKey[Int](r => 1,(sum,_) => sum+1,(l,r) => l+r)




    // average ratings of movies
    tupled.mapValues(t => t._1 / t._2).take(10).foreach(println)
    // RDD[(Int,Double)]
    val ratingsSum: RDD[(Int, Double)] = ratings.keyBy(_.movieId).mapValues(r => r.rating).reduceByKey(_ + _)

    val ratingsSum2: RDD[(Int, Double)] = keyedRatings.mapValues(_.rating).foldByKey(0d)(_ + _)

    val ratingsSum3: RDD[(Double, Int)] = ratingsSum2.map(_.swap)


    ratings.sortBy(_.rating).take(10).foreach(println)

    val partitioned = ratings.repartition(10)


    val movies = sc.textFile(s"${ParsingUtils.dPath}/movies.csv").filter(_!= "movieId,title,genres").map(ParsingUtils.parseMovie)


    val keyedMovies = movies.keyBy(_.id)

    val joined: RDD[(Int, (Movie, Rating))] = keyedMovies.join(keyedRatings)

    joined.take(10).foreach(println)

    val cogrouped: RDD[(Int, (Iterable[Movie], Iterable[Rating]))] = keyedMovies.cogroup(keyedRatings)

    // partition preserving

    //joined.mapPartitions()


  }
}
