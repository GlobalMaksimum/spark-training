# RDD on a spark cluster

<img src="rdd.png" width="400" height="400">

## Create rdd
```scala
val rdd:RDD[String] = sc.textFile("/some/path")
```
![img.png](img.png)

## transform it via flatmap narrow dependency
```scala
val rdd = sc.textFile("/some/path")
.flatMap(_.split(' '))
```
![img_1.png](img_1.png)

## transform it via map narrow dependency
```scala
val rdd = sc.textFile("/some/path")
  .flatMap(_.split(' '))
  .map(w=> (w,w.length))
```
![img_2.png](img_2.png)

## group by wide dependency
```scala
val rdd = sc.textFile("/some/path")
  .flatMap(_.split(' '))
  .map(w=> (w,w.length))
  .groupByKey()
```
![img_3.png](img_3.png)

## transform again narrow dependency
```scala
val rdd = sc.textFile("/some/path")
  .flatMap(_.split(' '))
  .map(w=> (w,w.length))
  .groupByKey()
  .mapValues(i => i.foldLeft((0,0)){case (z,v) => (z._1+v,z._2+1)} ).mapValues(i=>i._1.toDouble/i._2)
rdd.saveAsTextFile("/some/output/path")
```
![img_4.png](img_4.png)

## over all job
![img_5.png](img_5.png)

![img_6.png](img_6.png)

* Job:a set of tasks executed as a result of an action
* Stage:a set of tasks in a job that can be executed in parallel
* Task:an individual unit of work sent to one executor
* Application:the set of jobs managed by a single driver

# Dataframe/Dataset execution
![img_7.png](img_7.png)
