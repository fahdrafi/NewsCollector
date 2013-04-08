package com.twovix.newscollector

import au.com.bytecode.opencsv.CSVReader;
import scala.collection.parallel._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters
import scala.collection.mutable.Buffer
import java.io.FileReader
import com.mongodb._
import scala.concurrent.duration._

object NewsCollector {
  var mongoClient:MongoClient = _ 
  var db:DB = _
  
  def fetchRSSLinksF(plugin:RssPlugin) : Future[List[RSSItem]] = future {blocking {
	RssParser.parse(plugin)
    //	 (plugin.getItems()).toList
  }}
   def fetchRSSLinks(plugin:RssPlugin) : List[RSSItem] = {
	RssParser.parse(plugin)
    //	 (plugin.getItems()).toList
   }
   
   def constructRSSPlugin(path:String, plugin:String) : RssPlugin =  {  
	   (Class.forName(plugin)
						.getConstructors()(0).newInstance(path)).asInstanceOf[RssPlugin];
   }
   
   def constructRSSPluginF(path:String, plugin:String) : Future[RssPlugin] = future {  
	   (Class.forName(plugin)
						.getConstructors()(0).newInstance(path)).asInstanceOf[RssPlugin];
  }
  def insertStory(i:RSSItem) = {
    if(db.getCollection("newslinks").find(new BasicDBObject("link", i.Link)).count() == 0) {
      println("Adding new article: " + i.Title)
      db.getCollection("newslinks").insert(
								new BasicDBObject("link", i.Link)
								.append("date", i.Date)
								.append("title", i.Title)
								.append("description", i.Description)
								);
    }
  }
  
  def insertLinks(list:List[RSSItem]) = {
    for(l <- list)
      insertStory(l)
  }
  
   
  private def updateFeeds(feedsPath: String): Unit = {
    println("Loading feeeds from" + feedsPath)
    mongoClient = new MongoClient("localhost")
    db = mongoClient.getDB("newsdb")
    //Reading in feeds
    val csvReader = new CSVReader(new FileReader(feedsPath))
    var next = true
    while (next) {
        val nextLine = csvReader.readNext()
        if (nextLine != null) insertRssFeed(nextLine)
        else next = false
      }
    csvReader.close()
  }
  
  private def loadFeeds(): List[(String,String)] = {
     var feedList: List[(String,String)] = Nil
	  val feedCursor = db.getCollection("rssfeeds").find()
	  while(feedCursor.hasNext()) {
		val feedObject = feedCursor.next()
		val feedLink = feedObject.get("path").toString()
		val feedPlugin = feedObject.get("plugin").toString()
		feedList ::=  (feedLink,feedPlugin)	  
	  }
     feedList
  }
  def insertRssFeed(rssArr:Array[String]) =  {   
    val rssPath = rssArr(0)
    val rssPlugin =  "com.twovix.newscollector." + 
    		(rssArr.length match {
    			case 1 => "RssPluginStandard"
    			case 2 => rssArr(1) match {
    			  	case "" => "RssPluginStandard"
    			  	case x => x
    			}
    			case _ => throw new Exception("Malformed entry in RSS csv file")
    		})
    if(db.getCollection("rssfeeds").find(new BasicDBObject("path", rssPath)).count() == 0){
      db.getCollection("rssfeeds").insert(
						new BasicDBObject("path", rssPath)
						.append("plugin", rssPlugin));
    }
  }
  
  def main(args: Array[String]) =  {
	  if(!args.exists(_ == "--feeds")) {
	    throw new Exception("Must specify --feeds path")
	  } 
	  val feedsPath = args(args.indexOf("--feeds") + 1)
	  updateFeeds(feedsPath)
	  val feedList = loadFeeds().map(l=> constructRSSPlugin(l._1, l._2)).par
	  feedList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(4))
	  
	while(true){
	  println("Entering update loop for " + feedList.length + " feeds" )
/*	Futures version 
 *  val pluginFList = feedList.map(l =>  constructRSSPlugin(l._1,l._2))
	 val linkFList = pluginFList.map(_.flatMap(fetchRSSLinks(_)))
	 linkFList.map(_ onComplete {
	 	case Success(p) => insertLinks(p)
    	case Failure(t) => {println("Failed plugin construction " + t.getMessage() )}
    	}) 
      val t = linkFList.map(p => Await.ready(p, 1200 seconds))
      * */
	 feedList.map(p => insertLinks(fetchRSSLinks(p)))
  /*   val pluginList = feedList.map(l =>  constructRSSPlugin(l._1,l._2))
     println("Constructed plugins")
     pluginList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(64))
     val linkFList = pluginList.map(fetchRSSLinks(_))
     println("Fetched links")
     linkFList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(64))
	 linkFList.map(insertLinks(_))
	 println("Inserted Links")
	 * */
     Thread.sleep(1000)
	}
  }

}