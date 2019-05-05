

import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.{SparkConf, SparkContext}


object api {

  val accessToken = "1094675114891845632-TEn7tI11ZEyshO4pkP5W7dw6KySWHJ"
  val accessSecret = "8T1esyezmrUcGmncQ9X6eGN51lYodG1cuJjPm4tIi0pRN"
  val consumerKey = "r8BqAgfNYXSVnlElPuAlfRCVT"
  val consumerSecret = "W2tW9j6SmKG59qn2xMwP6ojhBe6g3DzKk4CmmRQtOl3VE1RxzA"


  def main(args: Array[String]) {

    val consumer = new CommonsHttpOAuthConsumer(consumerKey, consumerSecret)
    consumer.setTokenWithSecret(accessToken, accessSecret)


    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // Contains SQLContext which is necessary to execute SQL queries
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Reads json file and stores in a variable
    val tweet = sqlContext.read.json("/home/koushik/Desktop/vidyu/tweets.json")

    //To register tweets data as a table
    tweet.createOrReplaceTempView("tweets")
    println("Below are the 10 Screen Names from Tweets file")
    val users = sqlContext.sql("SELECT distinct user.screen_name as User_Screen_Name from tweets LIMIT 10")
    users.show()
    println("Enter your Screen Name to get Follower Id's: ")
    val name = scala.io.StdIn.readLine()

    val request = new HttpGet("https://api.twitter.com/1.1/followers/ids.json?cursor=-1&screen_name=" + name)
    consumer.sign(request)
    val client = new DefaultHttpClient()
    val response = client.execute(request)

    println(response.getStatusLine().getStatusCode());
    println(IOUtils.toString(response.getEntity().getContent()))
  }
}


