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
 def jshtml2text( html:String) : String = {
    return (Jsoup.parse(html).text())
  }
}

object RssParser {     
	def parse(rssText:String, plugin:RssPlugin) : List[RssItem] =  {
		if( !(rssText.contains("<rss") && rssText.contains("</rss>"))) return Nil
		
	    val dateFormatterRssPubDate = new java.text.SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z", java.util.Locale.ENGLISH);
	    val altDateFormatter =   new java.text.SimpleDateFormat("dd MMM yyyy", java.util.Locale.ENGLISH);
 		var itemList: List[RssItem] = Nil
		val rssxml = XML.loadString(rssText)
		val itemsxml = rssxml \\ "item"

		for(i <- itemsxml) {
		  val  titem = new RssItem
		  try{
		    titem.Date = dateFormatterRssPubDate.parse((i \ "pubDate").text)
		  } catch {
		    case e:java.text.ParseException => titem.Date =  new java.util.Date()
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
	    val urlCon = url.openConnection().asInstanceOf[java.net.HttpURLConnection]
	    urlCon.setConnectTimeout(2500)
	    urlCon.setReadTimeout( 2000 )
	    try {
	      fromInputStream( urlCon.getInputStream ).getLines.mkString("\n")
	    } finally {
	      urlCon.disconnect()
	    }
	}
	
	def parse(plugin:RssPlugin) : List[RssItem] = {
		try{	    
			val wstring = read(plugin.getPath())
			val xlist = parse(wstring, plugin)
			return xlist
		} catch {
			case e:Throwable=>
	      //Remove broken feeds from database?
		}
		Nil    
	}
}