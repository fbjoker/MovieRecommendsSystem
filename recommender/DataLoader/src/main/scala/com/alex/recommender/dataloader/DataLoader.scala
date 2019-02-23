package com.alex.recommender.dataloader


import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.action.ActionFuture
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.{IndicesExistsRequest, IndicesExistsResponse}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.util.matching.Regex

/**
  * movie 数据集以^分割
  * 260
  * ^Star Wars: Episode IV - A New Hope (1977)
  * ^Princess Leia is captured and held hostage by the evil Imperial forces in their effort to take over the galactic Empire. Venturesome Luke Skywalker and dashing captain Han Solo team together with the loveable robot duo R2-D2 and C-3PO to rescue the beautiful princess and restore peace and justice in the Empire.
  * ^121 minutes
  * ^September 21, 2004
  * ^1977^English
  * ^Action|Adventure|Sci-Fi
  * ^Mark Hamill|Harrison Ford|Carrie Fisher|Peter Cushing|Alec Guinness|Anthony Daniels|Kenny Baker|Peter Mayhew|David Prowse|James Earl Jones|Phil Brown|Shelagh Fraser|Jack Purvis|Eddie Byrne|Denis Lawson|Garrick Hagon|Don Henderson|Leslie Schofield|Richard LeParmentier|Michael Leader|Alex McCrindle|Drewe Henley|Jack Klaff|William Hootkins|Angus MacInnes|Jeremy Sinden|Graham Ashley|David Ankrum|Mark Austin|Scott Beach|Lightning Bear|Jon Berg|Doug Beswick|Paul Blake|Janice Burchette|Ted Burnett|John Chapman|Gilda Cohen|Tim Condren|Barry Copping|Alfie Curtis|Robert Davies|Maria De Aragon|Robert A. Denham|Frazer Diamond|Peter Diamond|Warwick Diamond|Sadie Eden|Kim Falkinburg|Harry Fielder|Ted Gagliano|Salo Gardner|Steve Gawley|Barry Gnome|Rusty Goffe|Isaac Grand|Nelson Hall|Reg Harding|Alan Harris|Frank Henson|Christine Hewett|Arthur Howell|Tommy Ilsley|Joe Johnston|Annette Jones|Linda Jones|Joe Kaye|Colin Michael Kitchens|Melissa Kurtz|Tiffany L. Kurtz|Al Lampert|Anthony Lang|Laine Liska|Derek Lyons|Mahjoub|Alf Mangan|Rick McCallum|Grant McCune|Geoffrey Moon|Mandy Morton|Lorne Peterson|Marcus Powell|Shane Rimmer|Pam Rose|George Roubicek|Erica Simmons|Angela Staines|George Stock|Roy Straite|Peter Sturgeon|Peter Sumner|John Sylla|Tom Sylla|Malcolm Tierney|Phil Tippett|Burnell Tucker|Morgan Upton|Jerry Walter|Hal Wamsley|Larry Ward|Diana Sadley Way|Harold Weed|Bill Weston|Steve 'Spaz' Williams|Fred Wood|Colin Higgins|Mark Hamill|Harrison Ford|Carrie Fisher|Peter Cushing|Alec Guinness
  * ^George Lucas
  *
  **/
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String,
                 directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

/**
  * mongdb 的连接配置
  *
  * @param uri 连接
  * @param db  数据库
  */
case class MongoConfig(uri: String, db: String)

/**
  * 连接ES的配置
  *
  * @param httpHosts
  * @param transportHosts
  * @param index
  * @param clustername
  */
case class ESConfig(httpHosts: String, transportHosts: String, index: String,
                    clustername: String)


object DataLoader {

  // 以window下为例，需替换成自己的路径，linux下为 /YOUR_PATH/resources/movies.csv
  val MOVIE_DATA_PATH = "D:\\Workspace\\recommender\\movies.csv"
  val RATING_DATA_PATH = "D:\\Workspace\\recommender\\ratings.csv"
  val TAG_DATA_PATH = "D:\\Workspace\\recommender\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"


  def main(args: Array[String]): Unit = {

    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.1.106:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "hadoop102:9200",
      "es.transportHosts" -> "hadoop102:9300,hadoop103:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "my-es"
    )


    val sparkConf: SparkConf = new SparkConf().setMaster(config.get("spark.cores").get).setAppName("recommender1")
    //val sc = new SparkContext(sparkConf)
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._



    //加载数据
    val movieRdd: RDD[String] = sparkSession.sparkContext.textFile(MOVIE_DATA_PATH)
    val ratingRdd: RDD[String] = sparkSession.sparkContext.textFile(RATING_DATA_PATH)
    val tagRdd: RDD[String] = sparkSession.sparkContext.textFile(TAG_DATA_PATH)

    //将rdd转为DF
    val movieDF: DataFrame = movieRdd.map { item =>

      val attr: Array[String] = item.split("\\^")
      //直接转换为movie对象
      Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim,
        attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim
      )

    }.toDF()
    val ratingDF = ratingRdd.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()
    val tagDF = tagRdd.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

    // 声明一个隐式的配置对象
    implicit val mongoConfig =
      MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)



    //保存到数据库mongodb
    //storeDataInMongoDB(movieDF, ratingDF, tagDF)


    import org.apache.spark.sql.functions._

    //        希望tag转换为  mid, tags(tag1|tag2...)这样的数据格式
    val newTagDF: DataFrame = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")
    //注意第二个参数需要seq, 把movie和tag连接成一个表提高检索效率
    val movietagsDF: DataFrame = movieDF.join(newTagDF, Seq("mid"), "left")


    implicit val eSConfig = ESConfig(config.get("es.httpHosts").get,
                                      config.get("es.transportHosts").get,
                                      config.get("es.index").get,
                                      config.get("es.cluster.name").get
    )
    //保存到ES
    storeDataInES(movietagsDF)



    //关闭spark
    sparkSession.stop()


  }

  def storeDataInES(movieDF:DataFrame)(implicit eSConfig: ESConfig): Unit = {
    //配置
    val settings: Settings = Settings.builder().put("cluster.name",eSConfig.clustername).build()
    //新建一个ES客户端
    val client = new PreBuiltTransportClient(settings)

    //需要将TransportHosts添加到esClient中
    val REGEX_HOST_PORT: Regex = "(.+):(\\d+)".r

    eSConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String,port:String)=>{
        //注意新老API的区别
        //client.addTransportAddress( new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
        client.addTransportAddress( new TransportAddress(InetAddress.getByName(host),port.toInt))
      }

    }

    println(client)
    //如果有数据就清除原来的数据
    if(client.admin().indices().exists(new IndicesExistsRequest(eSConfig.index)).actionGet().isExists){
      client.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }

    client.admin().indices().create(new CreateIndexRequest(eSConfig.index))

    //写入到ES

    movieDF.write
      .option("es.nodes",eSConfig.httpHosts)
      .option("es.http.timeout","100m")
      .option("es.mapping.id","mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index+"/"+ES_MOVIE_INDEX)

    client.close()


  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    //新建连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //如果MongoDB中有对应的数据库，那么应该删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()


    movieDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    tagDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

//    //建立索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    //关闭MongoDB的连接
    mongoClient.close()


  }


}
