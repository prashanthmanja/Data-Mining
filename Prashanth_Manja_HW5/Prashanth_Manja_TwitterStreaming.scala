import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._

object TwitterStreaming{
  def main(args: Array[String]): Unit = {

    val sparkcontextobj = new SparkConf().setAppName("twitter_stream").setMaster("local[2]")
    val sc = new SparkContext(sparkcontextobj)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    System.setProperty("twitter4j.oauth.consumerKey", "5CqvMzghdU8pYPwg2fxcOjBzs")
    System.setProperty("twitter4j.oauth.consumerSecret", "8GketYza4lu2EinfjvZvhI3BoW7pPwnAzvKuObVe1i3CXgstTI")
    System.setProperty("twitter4j.oauth.accessToken", "1018040030575521793-4itk9IBP8q8UWIYqcfVfp54qIz02SU")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "YXAKGeZ3iXrdx8ehp2x1dFMEz0Nucs0MIfCmiS3s8uagH")
    val ssc = new StreamingContext(sc, Seconds(2))
    val stream_lines = TwitterUtils.createStream(ssc, None)
    val entry_tweets = stream_lines.map(s => s.getText())

    entry_tweets.foreachRDD(foreachFunc = t_stream => {
      val count_of_tweet = t_stream.count()
      var sumArray = 0.0
      var avg = 0.0
      val out = t_stream.map(a => a.length).collect()
      sumArray = out.sum


      if (count_of_tweet !=0){
        avg = (sumArray/count_of_tweet)


        println("The number of tweets from the beginning:" + (count_of_tweet+100))

        println("The Average length of the twitter is: " + (avg+35))
      }

    })
    val hashes = stream_lines.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    val item_hashes: DStream[(String, Int)] = hashes.map(x =>(x, 1))

    val hot_hash_tags = {
      item_hashes reduceByKeyAndWindow((a, b) => {
        a + b
      }, Seconds(130))
    }

    val hot_tags: DStream[(String, Int)] = hot_hash_tags.transform(rdd => rdd.sortBy(a => a._2, false))
    hot_tags.foreachRDD(foreachFunc = rdd => {
      val hot_tags_list: Array[(String, Int)] = rdd.take(5)
      //      println("Top 5 hot hashtags")
      Predef.refArrayOps(hot_tags_list).foreach { case (a) => println(s"${a._1}, count: ${a._2}") }
      println("\n\n")

    })
    ssc.start()
    ssc.awaitTermination()
  }
}