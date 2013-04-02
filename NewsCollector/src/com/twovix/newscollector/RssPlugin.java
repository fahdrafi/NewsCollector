package com.twovix.newscollector;

import java.util.ArrayList;
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
	
	public List<Item> getItems() {
		List<Item> list = new ArrayList<Item>();
		for (RssItemBean item : feed.getItems()) {
			Item i = new Item();
			i.Title = item.getTitle();
			i.Date = item.getPubDate();
			i.Link = item.getLink();
			list.add(i);
		}
		return list;
	}
}
