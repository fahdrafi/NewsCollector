package com.twovix.newscollector;

public class RssPluginGoogle extends RssPlugin {

	public RssPluginGoogle(String path) throws Exception {
		super(path);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected String getLink(String link) {
		// TODO Auto-generated method stub
		return link.split("url=")[1];
	}

}
