package com.twovix.newscollector

import scala.xml._
import scala.xml.factory.XMLLoader
import org.xml.sax._
import org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
import scala.xml.PrettyPrinter
import org.jsoup._
import java.net.{URLConnection, URL}
import scala.io.Source.{fromInputStream}

object htmlcleaner {
  val parserFactory = new org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
  val parser = parserFactory.newSAXParser()
  val adapter = new scala.xml.parsing.NoBindingFactoryAdapter 
  
  def jshtml2text( html:String) : String = {
    return (Jsoup.parse(html).text())
  }
}

object RssParser {
   val dateFormatterRssPubDate = new java.text.SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z", java.util.Locale.ENGLISH);
   val altDateFormatter =   new java.text.SimpleDateFormat("dd MMM yyyy", java.util.Locale.ENGLISH);
   
   
	def parse(rssText:String, plugin:RssPlugin) : List[RSSItem] = {
		var itemList: List[RSSItem] = Nil
		val rssxml = XML.loadString(rssText)
		val itemsxml = rssxml \\ "item"
		for(i <- itemsxml) {
		  val  titem = new RSSItem
		  try{
		    titem.Date = dateFormatterRssPubDate.parse((i \ "pubDate").text)
		  } catch {
		    case e:java.text.ParseException =>
		  }
		  titem.Link = plugin.getLink((i \ "link").text)
		  titem.Title =( i \ "title").text
		  titem.Description = htmlcleaner.jshtml2text((i \ "description").text)
		  //htmlcleaner.html2text(titem.Description)
		  itemList ::= titem
		}
		itemList
	}
	
	def read(sturl:String):String = {
	    val url = new URL(sturl)
	    val urlCon = url.openConnection()
	    urlCon.setConnectTimeout(1500)
	    urlCon.setReadTimeout( 1000 )	
	    fromInputStream( urlCon.getInputStream ).getLines.mkString("\n")
	}
	
	def parse(plugin:RssPlugin) : List[RSSItem] = {
	
	try{	    
	  val wstring = read(plugin.getPath())
	  val xlist = parse(wstring, plugin)
	  return xlist
	} catch {
	  case e:Throwable=> println("Exception in parser: " + e.toString() + 
	      " for path " + plugin.getPath()) 
	      //Remove broken feeds from database?
	}
	Nil    
	}
	def main(args: Array[String]) {

	  val wstring = read("http://therail.blogs.nytimes.com/feed/")
	 // htmlcleaner.html2text(wstring)
	  htmlcleaner.jshtml2text(wstring)
	  val xlist = parse(wstring, new RssPluginStandard(""))
	 // xlist.map(x => println(x.Date + "  " + x.Link +  "  " + x.Title + "\n" + x.Description))
	}
}