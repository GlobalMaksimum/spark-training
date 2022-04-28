package training

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object IntroDataFrames {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().master("local[*]").appName("IntroRDD").getOrCreate()

    val df = spark.range(100000000).toDF("number")

    println(df.count())
    // TODO fix paths
    val movies = spark.read.json(s"/data/movies.json")

    import spark.implicits._

    val moviesDS:Dataset[Movie] = movies.as[Movie]



    val filteredMovieDS = moviesDS.where(col("movieId") > 3).select(col("id"),col("genres"))

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

    movies.select(col("movieId")).distinct()

    movies.sortWithinPartitions()

    movies.cache()

    //movies.as[Movie].toDF()

    RowEncoder
    //type DataFrame = Dataset[]

        /*
    docker run --rm --name sparkshell -it --network environment_default -p 4040:4040 -v `pwd`/data/small:/data -v `pwd`/app:/app capacman/spark-shell:3.2.1  spark-shell --master spark://spark-master:7077 --packages io.delta:delta-core_2.13:1.2.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf spark.hadoop.hive.metastore.uris=thrift://metastore:9083 --conf spark.sql.warehouse.dir=/data/warehouse --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict
    */
    /*
    docker run --rm --name sparkshell -it --network environment_default -p 4040:4040 -v `pwd`/data/small:/data -v `pwd`/app:/app capacman/spark-shell:3.2.1  spark-shell --master spark://spark-master:7077  --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.1 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog --conf spark.sql.catalog.spark_catalog.type=hive --conf spark.hadoop.hive.metastore.uris=thrift://metastore:9083 --conf spark.sql.warehouse.dir=/data/warehouse --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict

    */


    /*
    docker run --rm --name sparkshell -it --network environment_default -p 4040:4040 -v `pwd`/data/small:/data -v `pwd`/app:/app capacman/spark-shell:3.2.1_2.12  spark-shell --master spark://spark-master:7077 --conf spark.hadoop.hive.metastore.uris=thrift://metastore:9083 --conf spark.sql.warehouse.dir=/data/warehouse --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict
    */


    /*
    Hive Catalog + SQLLike(HiveQL)   => DB + TABLE + VIEW(Mysql like)   HIVEQL => MR Transpile

    database => table + view

    0.13  =>  1.x => 2.x => 3.x

    metastore db(mysql,postgresql,mssql,oracle)  <-  

    metastore db <- Hive metastore(Catalog) expose thrift + hiveserver + client(jdbc,odbc) beeline
    hiveserver => transpile mr || transpile tez || transpile spark  (hive execution engine hive.engine)

    (spark + impala + presto + flink + apache drill) => Hive Metastore thrift(remote) || metastoredb jdbc direct access(local)
    */
  }
}
