package com.twovix.newscollector

abstract class RssPlugin (path:String) {

	def getPath():String =  {
		return path;
	}

	def getLink(link:String):String 
}

class RssPluginGoogle (path:String) extends RssPlugin (path) {
  	override def getLink(link:String):String = {	
  		link.split("url=")(1);
  	}
}

class RssPluginStandard  (path:String) extends RssPlugin (path) {
  
  override def getLink(link:String):String = {
	 link
  }
}