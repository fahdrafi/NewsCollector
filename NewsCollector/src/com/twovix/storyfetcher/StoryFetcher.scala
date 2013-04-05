package com.twovix.storyfetcher

import java.net.{URLConnection, URL}
import scala.xml.{Elem, XML}
import scala.xml.factory.XMLLoader
import org.xml.sax._
import org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
import scala.collection.mutable.Map
 


trait SiteLoader {
  val parserFactory = new org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
  val parser = parserFactory.newSAXParser()
  val adapter = new scala.xml.parsing.NoBindingFactoryAdapter

  def loadXMLFromURLString(mURL:String):scala.xml.Node  = {
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
    for(p <- ( pageXML \\ "div")) {
      val divclass = p \ "@class"
      if(divclass.text == "story-body")
        return p
    }
    //if story-body not found, return entire page
    return pageXML
  }  
}

object TestBBC {
	def main(args: Array[String]) {
    val WordMap =  RawLoader.getWordMapFromURLString("http://www.bbc.co.uk/news/uk-scotland-22015175")
    for(t <- WordMap)
      println(t._1 + "\t" + t._2)
  }
}