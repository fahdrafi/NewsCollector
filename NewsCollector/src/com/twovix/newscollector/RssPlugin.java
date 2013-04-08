package com.twovix.newscollector;


public abstract class RssPlugin {

	private String path = "";

	public String getPath() {
		return path;
	}
	
	public RssPlugin(String rpath){
		path = rpath;
	}
	
	protected String getLink(String link) {
		return link;
	}
}
