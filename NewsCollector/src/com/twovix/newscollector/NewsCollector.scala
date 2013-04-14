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
import com.google.common.cache._
import scala.collection.mutable._


object NewsCollector {
  var mongoClient:MongoClient = _ 
  var db:DB = _
  
  
  
  class linkCacheT extends scala.collection.mutable.HashSet[String] with
      scala.collection.mutable.SynchronizedSet[String] {
    var hits = 0
    var misses = 0
    
    def incHits = this.synchronized {
    	hits += 1
    }
    def incMisses = this.synchronized {
    	misses += 1
    }
   }
  val linkCache:linkCacheT = new linkCacheT

  

   def fetchRSSLinks(plugin:RssPlugin) : List[RSSItem] = {
	RssParser.parse(plugin)
    //	 (plugin.getItems()).toList
   }
   
   def constructRSSPlugin(path:String, plugin:String) : RssPlugin =  {  
	   (Class.forName(plugin)
						.getConstructors()(0).newInstance(path)).asInstanceOf[RssPlugin];
   }
   

  def insertStory(i:RSSItem) : Boolean = {
	if (linkCache.size > 100000) 
	  {
		linkCache.clear
		println("Clearing Link Cache")
	  }
    if(!linkCache.contains(i.Link)) {
	  linkCache.add(i.Link)
	  linkCache.incMisses 
	  if(db.getCollection("newslinks").find(new BasicDBObject("link", i.Link)).count() == 0) {
			db.getCollection("newslinks").insert(
								new BasicDBObject("link", i.Link)
								.append("date", i.Date)
								.append("title", i.Title)
								.append("description", i.Description)
								);
			true
	  } else { 
	    false
	  }
	} else {
		linkCache.incHits
		false
	}
  }
  
  def insertLinks(list:List[RSSItem]):Int = {
   list.map(x=>insertStory(x)).count(_ == true)
  }
  
   
  private def updateFeeds(feedsPath: String): Unit = {
    println("Loading feeeds from" + feedsPath)
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

	mongoClient = new MongoClient("localhost")
    db = mongoClient.getDB("newsdb")
    updateFeeds(feedsPath)
    
	val feedList = loadFeeds().map(l=> constructRSSPlugin(l._1, l._2)).par
	feedList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(16))
	var loopcounter:Long = 1
	while(true){
	 println(loopcounter + "  Entering update loop for " + feedList.length + " feeds\nHits:" + linkCache.hits + "\nMisses" + linkCache.misses + "\n" )
	 val insCount = feedList.map(p => insertLinks(fetchRSSLinks(p))).reduceLeft(_+_)
	 loopcounter+=1
	 println("Inserted " + insCount + " stories")
     Thread.sleep(300000)
	}
  }

}