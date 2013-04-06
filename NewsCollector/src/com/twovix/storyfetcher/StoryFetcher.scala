package com.twovix.storyfetcher

import java.net.{URLConnection, URL}
import scala.io.Source.{fromInputStream}
import scala.xml.{Elem, XML, Node}
import scala.xml.factory.XMLLoader
import org.xml.sax._
import org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
import scala.collection.mutable.Map
import com.mongodb._;

 


trait SiteLoader {
  val parserFactory = new org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
  val parser = parserFactory.newSAXParser()
  val adapter = new scala.xml.parsing.NoBindingFactoryAdapter

  def attributeEquals(name:String, value: String)(node: Node) = {
    val rval = node.attributes.exists(x => (x.key == name && x.value.text == value))
    		if(rval)
    		  println("F: " + name + "  " + value + node)
    rval
  }
  
  def loadXMLFromURLString(mURL:String):scala.xml.Node  = {
    val url = new URL(mURL)
    val urlCon = url.openConnection()
    urlCon.setConnectTimeout(2000)
    urlCon.setReadTimeout( 1000 )	
    val content = fromInputStream( urlCon.getInputStream ).getLines.mkString("\n")
    val source = new org.xml.sax.InputSource(new URL(mURL).openStream())
    val feed = adapter.loadXML(source, parser)
    feed
  }

  def debugSource(sourceXML:scala.xml.NodeSeq) = {
    println(sourceXML)
  }

  def getWordMapFromURLString(mURL:String):scala.collection.mutable.Map[String,Int] = {
    val WordMap = Map[String,Int]()  
    val pageXML =   getStoryBody(loadXMLFromURLString(mURL))
    debugSource(pageXML)
    for( p <- ( pageXML \\ "p") ) {
      val words = p.text.split("[ ,!.\"\']")
      for(w<- words)
        (WordMap(w.toLowerCase) = WordMap.getOrElse(w.toLowerCase, 0 ) + 1)
    }
    WordMap
  }

  def getStoryBody(pageXML:scala.xml.Node):scala.xml.NodeSeq

}

object RawLoader extends SiteLoader {
  override def getStoryBody(pageXML:scala.xml.Node):scala.xml.NodeSeq = {
    // return entire page
    return pageXML
  }  
}

object BBCLoader extends SiteLoader {
  override def getStoryBody(pageXML:scala.xml.Node):scala.xml.NodeSeq = {
    return  (pageXML \\ "div").filter(attributeEquals("class","story-body"))
  }  
}

object NYTLoader extends SiteLoader {
  override def getStoryBody(pageXML:scala.xml.Node):scala.xml.NodeSeq = {
    return  (pageXML \\ "div").filter(attributeEquals("class","articleBody"))
  }  
}
object ReutersLoader extends SiteLoader {
  override def getStoryBody(pageXML:scala.xml.Node):scala.xml.NodeSeq = {
    return  (pageXML \\ "span" ).filter(attributeEquals("id","articleText"))
  }    
}
object ScrapeLinks {
	def main(args: Array[String]) {
	val mongoClient = new MongoClient("localhost")
	val db = mongoClient.getDB("newsdb")
	val links = db.getCollection("newslinks")
	val linkCursor = links.find()
	
	while(linkCursor.hasNext()) {
		val linkObject = linkCursor.next()
		val link = linkObject.get("link").toString()
		println(link)
		try{
		  val WordMap =  RawLoader.getWordMapFromURLString(link)
		} catch {
		  case e:java.net.SocketTimeoutException => println("Connection timed out")
		}
	}
    //val WordMap =  ReutersLoader.getWordMapFromURLString("http://uk.reuters.com/article/2013/04/05/uk-korea-north-idUKBRE93407U20130405")
    //val sortedFreq = WordMap.toSeq.sortBy(_._1)
    //for(t <- sortedFreq)
    //  println(t._1 + "\t" + t._2)
    //  println("All done")
  }
}