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
			DB db = mongoClient.getDB("test");
			String[] nextLine = null;
			while ((nextLine = csvReader.readNext())!=null) {
				String rssPath = nextLine[0];
				String plugin = "com.twovix.newscollector." + (nextLine.length>1 ? nextLine[1]:"RssPluginStandard");

				RssPlugin rssPlugin = (RssPlugin) Class.forName(plugin).getConstructor(String.class).newInstance(rssPath);

				for (RssPlugin.Item item : rssPlugin.getItems()) {
					if(db.getCollection("newsdocs").find(new BasicDBObject("link", item.Link)).count()>0) continue;
					db.getCollection("newsdocs").insert(
							new BasicDBObject("link", item.Link)
							.append("date", item.Date)
							.append("title", item.Title)
							);
					System.out.println("Inserted Link: " + item.Link);
				}
			}
			csvReader.close();
			mongoClient.close();
			Thread.sleep(60000);
		}

	}

}
