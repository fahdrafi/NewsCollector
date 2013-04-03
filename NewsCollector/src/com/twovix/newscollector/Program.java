package com.twovix.newscollector;

import au.com.bytecode.opencsv.CSVReader;

import com.mongodb.*;

import java.io.FileReader;
import java.util.Arrays;

public class Program {

	static MongoClient mongoClient = null;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		// TODO Auto-generated method stub
		if (!Arrays.asList(args).contains("--feeds")) throw new Exception("Must specify --feeds path");
		
		String feedsPath = args[Arrays.asList(args).indexOf("--feeds")+1];
		
		while (true) {
			CSVReader csvReader = new CSVReader(new FileReader(feedsPath));
			mongoClient = new MongoClient("localhost");
			DB db = mongoClient.getDB("newsdb");
			String[] nextLine = null;
			while ((nextLine = csvReader.readNext())!=null) {
				String rssPath = nextLine[0];
				String plugin = Program.class.getPackage().toString().split(" ")[1] + "." + (nextLine.length>1 ? nextLine[1]:"RssPluginStandard");
				
				if (db.getCollection("rssfeeds").find(
						new BasicDBObject("path", rssPath)
						).count() > 0) continue;
				
				System.out.println("Adding new rss feed: " + rssPath);
				db.getCollection("rssfeeds").insert(
						new BasicDBObject("path", rssPath)
						.append("plugin", plugin));
				
			}
			csvReader.close();
			
			DBCursor cursor = db.getCollection("rssfeeds").find();
			
			while (cursor.hasNext()) {
				DBObject rssFeedObj = cursor.next();
				String rssPath = (String) rssFeedObj.get("path");
				String rssPluginClass = (String) rssFeedObj.get("plugin");
				
				try {
					RssPlugin rssPlugin = (RssPlugin) Class.forName(rssPluginClass)
							.getConstructor(String.class).newInstance(rssPath);
					for (RssPlugin.Item item : rssPlugin.getItems()) {
						if(db.getCollection("newslinks")
								.find(new BasicDBObject("link", item.Link))
								.count()>0) 
							continue;
						System.out.println("Adding new article: " + item.Title);
						db.getCollection("newslinks").insert(
								new BasicDBObject("link", item.Link)
								.append("date", item.Date)
								.append("title", item.Title)
								);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.out.println("Could not parse: " + rssPath);
					continue;
				}
				
			}
			mongoClient.close();
			
			System.out.println("Sleeping ...");
			Thread.sleep(10000);
		}

	}

}
