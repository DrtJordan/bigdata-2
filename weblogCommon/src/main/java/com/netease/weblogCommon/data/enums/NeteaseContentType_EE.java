package com.netease.weblogCommon.data.enums;


public enum NeteaseContentType_EE{
	article("article"),
	photoSet("photoSet"),
	special("special"),
	video("video");
	
	private String name;

	private NeteaseContentType_EE(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}