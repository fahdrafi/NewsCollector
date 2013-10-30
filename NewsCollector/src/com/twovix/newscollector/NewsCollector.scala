package com.twovix.newscollector

import au.com.bytecode.opencsv.CSVReader
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
import org.apache.http.client._
import org.apache.http.client.methods._
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.impl.client.BasicResponseHandler
import org.apache.http.entity.StringEntity
import org.apache.http.entity.ContentType
import org.joda.time.DateTime
import org.joda.time._
import scala.xml.{Elem, XML, Node}
import scala.xml.factory.XMLLoader

object CalaisPost {

	val CALAIS_URL = "http://api.opencalais.com/tag/rs/enrich"
    val CALAIS_KEY = "t7bmcemzr7qc8yy5t4vkd895"
	
    def  run(text:String): String = {
		val httpClient = new DefaultHttpClient
		val httpPost = new HttpPost(CALAIS_URL)
		val brh = new BasicResponseHandler

		httpPost.addHeader("x-calais-licenseID", CALAIS_KEY);
		httpPost.addHeader("Content-Type", "text/raw; charset=UTF-8");
		httpPost.addHeader("Accept", "xml/rdf");
		httpPost.setEntity(new StringEntity(text, 
				           ContentType.create("text/plain", "UTF-8")))
		httpClient.execute (httpPost, brh);
	}

	def parseRdf(rdf:scala.xml.Elem) = (rdf \ "Description")
 								.map(p => Map(
 											"EntityID" -> (p.attributes.filter(a => a.key == "about").map(_.value.text)).head,
 											"EntityType" -> (p \ "type")(0).attributes.value.text,
 											"EntityName" -> (p \ "name").text
 										  )
		 						).filter(_("EntityName").size != 0).map(m => new BasicDBObject(m)); 
}

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

  

   def fetchRSSLinks(plugin:RssPlugin) : List[RssItem] = {
    RssParser.parse(plugin)
   }
   
   def constructRSSPlugin(path:String, plugin:String) : RssPlugin =  {  
	   (Class.forName(plugin)
						.getConstructors()(0).newInstance(path)).asInstanceOf[RssPlugin];
   }
   

  def insertStory(i:RssItem) : Boolean = {
	if (linkCache.size > 100000) 
	  {
		linkCache.clear
		println("Clearing Link Cache")
	  }
    if(!linkCache.contains(i.Link)) {
	  linkCache.add(i.Link)
	  linkCache.incMisses 
	  if(db.getCollection("newslinks").find(new BasicDBObject("link", i.Link)).count() == 0) {
		  val rdf = CalaisPost.run(i.Title + ". " + i.Description)
		  val entityList = CalaisPost.parseRdf(XML.loadString(rdf))
			db.getCollection("newslinks").insert(
								new BasicDBObject("link", i.Link)
								.append("date", i.Date)
								.append("title", i.Title)
								.append("description", i.Description)
								.append("rdf", rdf)
								.append("EntityList", entityList.toArray)
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
  
  def insertLinks(list:List[RssItem]):Int = {
   list.map(x=>insertStory(x)).count(_ == true)
  }
  
   
  private def updateFeeds(feedsPath: String): Unit = {
    println("Loading feeds from" + feedsPath)
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
    var dayCount = 0;
	val dailyLimit = 49000;
	var dayCountUpdated = DateTime.now
	val feedList = loadFeeds().map(l=> constructRSSPlugin(l._1, l._2)).par
	feedList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(1))
	var loopcounter:Long = 1
	while(true){
	 println(loopcounter + "  Entering update loop for " + feedList.length + " feeds\nHits:" + linkCache.hits + "\nMisses" + linkCache.misses + "\n" )
	 if (dayCount < dailyLimit) {
		 val insCount = feedList.map(p => insertLinks(fetchRSSLinks(p))).reduceLeft(_+_);
		 dayCount += insCount;	
		 loopcounter+=1;
		 println("Inserted " + insCount + " stories")
	 }
	 
	 if (dayCountUpdated.plusDays(1).isAfter(DateTime.now))  {
	   dayCount = 0;
	   dayCountUpdated = DateTime.now
	 }

     Thread.sleep(300000)
	}
  }
}