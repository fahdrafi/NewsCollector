package com.twovix.newsprocessor

import java.net.{URLConnection, URL}
import scala.io.Source.{fromInputStream}
import scala.xml.{Elem, XML, Node}
import scala.xml.factory.XMLLoader
import org.xml.sax._
import org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
import scala.collection.mutable.Map
import com.mongodb._;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf._;
import edu.stanford.nlp.io.IOUtils;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations;
 
class MapEntry {
  var count:Integer = 0
  var connections:Map[String,Int] = Map[String,Int]() 
}

 object WordGraph {
   
  val allEntities:Map[String, MapEntry] = Map[String,MapEntry]() 
  
  def size = allEntities.size
  
  def totalCount = allEntities.map(_._2.count).reduceLeft(_+_)
  
  def countWord(w:String):Unit = {
    val entry = allEntities.getOrElse(w,new MapEntry)
    entry.count += 1
    allEntities(w) = entry
  }
  
  def addConnection(w:String, c:String):Unit = {
    val entry = allEntities(w)
    entry.connections(c) = entry.connections.getOrElse(c,0) + 1
  }
  
  def addEntities(title:String, desc:String): Unit = {
	val str = title + ". " + desc
    val strXml = XML.loadString("<NER>" + WordTagger.nerString(str) + "</NER>")
 //   val tx = (strXml \ "wi").filter(_.attributes.exists( x=> {val tx = x.value.text; tx=="LOCATION" || tx == "PERSON" || tx =="ORGANIZATION"}))
    val tx = (strXml \ "_") 
    tx.map (t => { 
    	countWord(t.text)
    	tx.filter(_.text != t.text).map(x => addConnection(t.text,x.text))
    }) 
    
  }
  
  def topStories(count:Int):Map[String,MapEntry] = {
    val topNTerms = allEntities.toSeq.sortBy(_._2.count).reverse.take(count).map(_._1)
    println("Top Terms length = " + topNTerms.size)
    allEntities.filter(x => topNTerms.contains(x._1))
  }
  
  def getCount(s:String):Integer =  {
    allEntities.get(s) match {
      case Some(x) => x.count
      case None => 0   
    }
  }
  
  def getCountCaseless(s:String):Integer = {
    allEntities.filter( _._1.toLowerCase() == s.toLowerCase()).map(_._2.count).reduceLeft(_+_)
  }
  
  def getEdgesFrom(s:String):Map[String,Int] = {
    allEntities.get(s) match {
      case Some(x) => x.connections
      case None => Map[String,Int]()  
    }
  }
  
  def getEdge(s:String, d:String):Int = {
	 getEdgesFrom(s).get(d) match {
	   case Some(x) => x
	   case None => 0
	 }
  }
  
}

object WordCounter {

  def getWordMapFromString(str:String):scala.collection.mutable.Map[String,Int] = {
    val WordMap = Map[String,Int]()  
    val words = str.split("[ ,!.\"\']")
     for(w<- words)
        (WordMap(w.toLowerCase) = WordMap.getOrElse(w.toLowerCase, 0 ) + 1)
     WordMap
  }

}

object WordTagger {
   val serializedClassifier = "/Users/salmankhan/NewsCollector/NewsCollector/data/NER/english.all.3class.distsim.crf.ser.gz";
   val classifier:AbstractSequenceClassifier[CoreLabel]  = CRFClassifier.getClassifierNoExceptions(serializedClassifier);
  
  def nerString(str:String): String = {
    classifier.classifyWithInlineXML(str)
    //classifier.classifyToString(str, "xml", true)
  }

  
}


object WCMain {
	def main(args: Array[String]) {
	val mongoClient = new MongoClient("localhost")
	val db = mongoClient.getDB("newsdb")
	val links = db.getCollection("newslinks")
	var linkCursor = links.find()
	var countStories = 0
	val accumMap = Map[String,Int]()
	
	while(linkCursor.hasNext()) {
		val linkObject = linkCursor.next()
		val link = linkObject.get("link").toString()
		val title = linkObject.get("title").toString()
		val desc = linkObject.get("description").toString()
	
		
		try {
			WordGraph.addEntities(title, desc)
		} catch {
			case e:org.xml.sax.SAXParseException => println("Parse Exception")
		}
		countStories += 1
		if(countStories % 100 == 0 )
		  println(countStories)
	}
	println("Stories: "+ countStories)
	println("Caseless count: " + WordGraph.getCountCaseless("margaret thatcher"))
	println("thatcher count: " + WordGraph.getCount("margaret thatcher"))
	println("Thatcher count: " + WordGraph.getCount("Margaret Thatcher"))
	println("Unique words: " + WordGraph.size)
	println("Total words:" + WordGraph.totalCount)
	println("\n\nThatcher Connections:  \n\n\n" + WordGraph.getEdgesFrom("Thatcher").toSeq.sortBy(_._2))
	
	val top10 = WordGraph.topStories(10)
	println(top10.map(x => x._1.toString() + "  " + x._2.count.toString()+"\n"))
    //val WordMap =  ReutersLoader.getWordMapFromURLString("http://uk.reuters.com/article/2013/04/05/uk-korea-north-idUKBRE93407U20130405")
    //val sortedFreq = WordMap.toSeq.sortBy(_._1)
    //for(t <- sortedFreq)
    //  println(t._1 + "\t" + t._2)
    //  println("All done")
  }
}