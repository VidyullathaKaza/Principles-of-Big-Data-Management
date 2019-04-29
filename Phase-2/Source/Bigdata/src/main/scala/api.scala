

import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.{SparkConf, SparkContext}


object api {

  val accessToken = "779311765163171844-RCUoOhu2R53ugDk3O8xTX50rgi2zj4o"
  val accessSecret = "y9Evdnwz1tfI43fIyun18OQOxgt6HQjWh6g3Gb99ExwOI"
  val consumerKey = "xMJiyum9ZLKuGeZDPl1uL3qeU"
  val consumerSecret = "6df8h8k2O7AwBJgYREWwTfwB1MFXVBuUm4PttByrGiRKDj6bI5"


  def main(args: Array[String]) {

    val consumer = new CommonsHttpOAuthConsumer(consumerKey, consumerSecret)
    consumer.setTokenWithSecret(accessToken, accessSecret)


    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // Contains SQLContext which is necessary to execute SQL queries
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Reads json file and stores in a variable
    val tweet = sqlContext.read.json("/home/koushik/Desktop/Teju/Disease_Tweets.json")

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


