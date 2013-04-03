package com.twovix.newscollector;

import java.util.List;

import org.horrabin.horrorss.*;

public abstract class RssPlugin {
	protected RssParser parser = new RssParser();
	protected RssFeed feed = null;
	
	public class Item {
		public String Title;
		public java.util.Date Date;
		public String Link;
	}
	
	public RssPlugin(String path) throws Exception {
		feed = parser.load(path);
	}
	
	protected abstract String getLink(String link);
	
	public abstract List<Item> getItems();
}
