package training

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    // UN-D => compress => 8kb*4  -- native c,rust
    // 1000 * 8kb = 128mb*4 -- 521mb 32mb
    // snappy => direct =>
    // UN-D => compress => 128kb*4  -- native c,rust
    val spark = SparkSession.builder().appName("testApp").getOrCreate()

    val rdd  = spark.sparkContext.parallelize(1 to 1000000000)

    for(i <- 0 to 20) println(rdd.count())
    spark.close()
  }
}
