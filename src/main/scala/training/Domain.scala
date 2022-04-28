package training

import java.sql.Timestamp

case class Movie(
                  id: Long,
                  title: String,
                  year: Option[Long],
                  genres: List[String]
                )
case class Rating(userId: Int, movieId: Int, rating: Double, ts: Timestamp)

case class Link(movieId:Int,imdbId:Int,tmdbId:Option[Int])

case class Tag(userId:Int,movieId:Int,tag:String,ts:Timestamp)
