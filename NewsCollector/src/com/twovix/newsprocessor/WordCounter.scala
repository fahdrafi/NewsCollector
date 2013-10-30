package com.twovix.newsprocessor

import java.net.{URLConnection, URL}
import scala.io.Source.{fromInputStream}
import scala.xml.{Elem, XML, Node}
import scala.xml.factory.XMLLoader
import org.xml.sax._
import org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
import scala.collection.mutable.Map
import com.mongodb._
import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf._
import edu.stanford.nlp.io.IOUtils
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.CoreAnnotations
import com.github.nscala_time.time.Imports._



/************************************************************
	- Simple Calais client to process file or files in a folder
	- Takes 2 arguments
		1. File or folder name to process
		2. Output folder name to store response from Calais
	- Please specify the correct web service location url for CALAIS_URL variable
	- Please adjust the values of different request parameters in the createPostMethod
	
**************************************************************/
//

object rdfTest {
 val rdf = <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:c="http://s.opencalais.com/1/pred/"><rdf:Description c:calaisRequestID="f3422901-2f77-80a9-1420-b4c67a95642d" c:id="http://id.opencalais.com/y4yiKr0dPeHgvuBIfXb1*g" rdf:about="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613"><rdf:type rdf:resource="http://s.opencalais.com/1/type/sys/DocInfo"/><c:document><![CDATA[Editors 'must have known of hacking'. Former News of the World editors Rebekah Brooks and Andy Coulson must have known about phone hacking at the paper, the Old Bailey is told, as it emerges three former journalists on the paper have admitted charges.]]></c:document><c:docTitle/><c:docDate>2013-10-30 17:37:06.133</c:docDate></rdf:Description><rdf:Description c:contentType="text/raw" c:emVer="7.1.1103.5" c:langIdVer="DefaultLangId" c:language="English" c:processingVer="CalaisJob01" c:submissionDate="2013-10-30 17:37:05.961" rdf:about="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613/meta"><rdf:type rdf:resource="http://s.opencalais.com/1/type/sys/DocInfoMeta"/><c:docId rdf:resource="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613"/><c:submitterCode>f19fc52c-0e38-8486-bfda-46e9d709a6b2</c:submitterCode><c:signature>digestalg-1|a4DoWLtbDRl+MWlG7F3FHfpp3Gw=|FlPVfqWw3Du9PvkqpVqq1gcXKBYtlzTRz3P36ywMgy/dykvDkPEAKw==</c:signature></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613/lid/DefaultLangId"><rdf:type rdf:resource="http://s.opencalais.com/1/type/lid/DefaultLangId"/><c:docId rdf:resource="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613"/><c:lang rdf:resource="http://d.opencalais.com/lid/DefaultLangId/English"/></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/genericHasher-1/a14c8765-f2ba-366e-a9c4-e011b4d065b5"><rdf:type rdf:resource="http://s.opencalais.com/1/type/em/e/PublishedMedium"/><c:name>News of the World</c:name></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613/Instance/1"><rdf:type rdf:resource="http://s.opencalais.com/1/type/sys/InstanceInfo"/><c:docId rdf:resource="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613"/><c:subject rdf:resource="http://d.opencalais.com/genericHasher-1/a14c8765-f2ba-366e-a9c4-e011b4d065b5"/><!--PublishedMedium: News of the World; --><c:detection>['must have known of hacking'. Former ]News of the World[ editors Rebekah Brooks and Andy Coulson must]</c:detection><c:prefix>'must have known of hacking'. Former </c:prefix><c:exact>News of the World</c:exact><c:suffix> editors Rebekah Brooks and Andy Coulson must</c:suffix><c:offset>45</c:offset><c:length>17</c:length></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613/Relevance/1"><rdf:type rdf:resource="http://s.opencalais.com/1/type/sys/RelevanceInfo"/><c:docId rdf:resource="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613"/><c:subject rdf:resource="http://d.opencalais.com/genericHasher-1/a14c8765-f2ba-366e-a9c4-e011b4d065b5"/><c:relevance>0.357</c:relevance></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/pershash-1/d4dc20e3-79d1-38ae-bb44-62602f5b9f9e"><rdf:type rdf:resource="http://s.opencalais.com/1/type/em/e/Person"/><c:name>Andy Coulson</c:name><c:persontype>N/A</c:persontype><c:nationality>N/A</c:nationality><c:commonname>Andy Coulson</c:commonname></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613/Instance/2"><rdf:type rdf:resource="http://s.opencalais.com/1/type/sys/InstanceInfo"/><c:docId rdf:resource="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613"/><c:subject rdf:resource="http://d.opencalais.com/pershash-1/d4dc20e3-79d1-38ae-bb44-62602f5b9f9e"/><!--Person: Andy Coulson; --><c:detection>[News of the World editors Rebekah Brooks and ]Andy Coulson[ must have known about phone hacking at the]</c:detection><c:prefix>News of the World editors Rebekah Brooks and </c:prefix><c:exact>Andy Coulson</c:exact><c:suffix> must have known about phone hacking at the</c:suffix><c:offset>90</c:offset><c:length>12</c:length></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613/Relevance/2"><rdf:type rdf:resource="http://s.opencalais.com/1/type/sys/RelevanceInfo"/><c:docId rdf:resource="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613"/><c:subject rdf:resource="http://d.opencalais.com/pershash-1/d4dc20e3-79d1-38ae-bb44-62602f5b9f9e"/><c:relevance>0.357</c:relevance></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/pershash-1/da12fa7d-6d93-3035-9f1d-8ca1b8020ed0"><rdf:type rdf:resource="http://s.opencalais.com/1/type/em/e/Person"/><c:name>Rebekah Brooks</c:name><c:persontype>N/A</c:persontype><c:nationality>N/A</c:nationality><c:commonname>Rebekah Brooks</c:commonname></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613/Instance/3"><rdf:type rdf:resource="http://s.opencalais.com/1/type/sys/InstanceInfo"/><c:docId rdf:resource="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613"/><c:subject rdf:resource="http://d.opencalais.com/pershash-1/da12fa7d-6d93-3035-9f1d-8ca1b8020ed0"/><!--Person: Rebekah Brooks; --><c:detection>[of hacking'. Former News of the World editors ]Rebekah Brooks[ and Andy Coulson must have known about phone]</c:detection><c:prefix>of hacking'. Former News of the World editors </c:prefix><c:exact>Rebekah Brooks</c:exact><c:suffix> and Andy Coulson must have known about phone</c:suffix><c:offset>71</c:offset><c:length>14</c:length></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613/Relevance/3"><rdf:type rdf:resource="http://s.opencalais.com/1/type/sys/RelevanceInfo"/><c:docId rdf:resource="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613"/><c:subject rdf:resource="http://d.opencalais.com/pershash-1/da12fa7d-6d93-3035-9f1d-8ca1b8020ed0"/><c:relevance>0.357</c:relevance></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/comphash-1/2e324ae6-aeed-328f-8903-4d65815e4add"><rdf:type rdf:resource="http://s.opencalais.com/1/type/em/e/Company"/><c:name>News of the World</c:name><c:nationality>N/A</c:nationality></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613/Instance/4"><rdf:type rdf:resource="http://s.opencalais.com/1/type/sys/InstanceInfo"/><c:docId rdf:resource="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613"/><c:subject rdf:resource="http://d.opencalais.com/comphash-1/2e324ae6-aeed-328f-8903-4d65815e4add"/><!--Company: News of the World; --><c:detection>['must have known of hacking'. Former ]News of the World[ editors Rebekah Brooks and Andy Coulson must]</c:detection><c:prefix>'must have known of hacking'. Former </c:prefix><c:exact>News of the World</c:exact><c:suffix> editors Rebekah Brooks and Andy Coulson must</c:suffix><c:offset>45</c:offset><c:length>17</c:length></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613/Relevance/4"><rdf:type rdf:resource="http://s.opencalais.com/1/type/sys/RelevanceInfo"/><c:docId rdf:resource="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613"/><c:subject rdf:resource="http://d.opencalais.com/comphash-1/2e324ae6-aeed-328f-8903-4d65815e4add"/><c:relevance>0.357</c:relevance></rdf:Description><rdf:Description rdf:about="http://d.opencalais.com/er/company/ralg-tr1r/9544ffb8-6630-306a-87bc-5bb1a046345d"><rdf:type rdf:resource="http://s.opencalais.com/1/type/er/Company"/><c:docId rdf:resource="http://d.opencalais.com/dochash-1/550e5720-9540-3a4c-9313-42a85391f613"/><!--News of the World--><c:subject rdf:resource="http://d.opencalais.com/comphash-1/2e324ae6-aeed-328f-8903-4d65815e4add"/><c:score>1.0</c:score><c:name>NEWS OF THE WORLD LIMITED</c:name><c:shortname>News of the Wld</c:shortname></rdf:Description></rdf:RDF>
 val entites = (rdf \ "Description")
 								.map(p => (
 											(p.attributes.filter(a => a.key == "about").map(_.value.text)).head,
 											(p \ "type")(0).attributes.value.text,
 											(p \ "name").text
 										  )
		 						).filter(_._3.size != 0); 
}
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
  
  def openCalaisTagger(str:String): String = {
		  ""
  }
}


object WCMain {
	def main(args: Array[String]) {
	val mongoClient = new MongoClient("127.0.0.1")
	val db = mongoClient.getDB("newsdb")
	val links = db.getCollection("newslinks")
	
	var linkCursor = links.find()
	var countStories = 0
	val accumMap = Map[String,Int]()
	val dateParser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", java.util.Locale.ENGLISH);
	val formatter  = org.joda.time.format.DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss z yyyy");
	
	val now = DateTime.now
	val int1Hr = (now - 1.hours) to now
	val int12Hr = (now - 12.hours) to now
	val int24Hr = (now - 24.hours) to now
	val int72Hr = (now - 72.hours) to now

	var dateList:List[org.joda.time.DateTime] = Nil
	while(linkCursor.hasNext()) {
		val linkObject = linkCursor.next()
		val link = linkObject.get("link").toString()
		val title = linkObject.get("title").toString()
		val desc = linkObject.get("description").toString()
		val datestr = (linkObject.get("date").toString())
		val date =  new org.joda.time.DateTime(dateParser.parse(datestr))
		
		dateList =  date :: dateList
		
		try {
			WordGraph.addEntities(title, desc)
		} catch {
			case e:org.xml.sax.SAXParseException => println("Parse Exception: " + e.getMessage())
		}
		countStories += 1
		if(countStories % 100 == 0 )
		  println(countStories)
	}
	
	println("Last 1 hour: " + dateList.count(x => int1Hr.contains(x)))
	println("Last 12 hour: " + dateList.count(x => int12Hr.contains(x)))
	println("Last 24 hour: " + dateList.count(x => int24Hr.contains(x)))
	println("Last 72 hour: " + dateList.count(x => int72Hr.contains(x)))

	
	println("Stories: "+ countStories +  "  " + dateList.length)
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
	//	val rstr = CalaisPost.run("Rush hour train services are cancelled in parts of the country as one of the biggest storm in recent years is set to batter England and Wales on Sunday night and Monday.")
	//	println(rstr)
}
}