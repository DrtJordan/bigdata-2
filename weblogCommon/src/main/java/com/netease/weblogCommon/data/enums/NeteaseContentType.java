package com.netease.weblogCommon.data.enums;

import java.util.regex.Pattern;

public enum NeteaseContentType{
	artical("artical", "http://[\\w.]+\\.163\\.com/([^\\s]+/)?\\d{2}/\\d{4}/\\d{2}/[0-9A-Z]{8}00[0-9]{2}([0-9A-Z]{4}|sp|rt)(_\\d+|_all)?.html.*"),
	photo("photo", ".*/(photoview|photoset|photonew)/.*.html.*"),
	special("special", ".*/special/.*");
	
	public static final String OTHER = "other";
	private String name;
	private Pattern typePattern;

	private NeteaseContentType(String name, String regex) {
		this.name = name;
		this.typePattern = Pattern.compile(regex);
	}

	public String getName() {
		return name;
	}

	public boolean match(String url) {
		return typePattern.matcher(url).matches();
	}

	public static String getTypeName(String url) {
		for (NeteaseContentType val : NeteaseContentType.values()) {
			if (val.match(url)) {
				return val.getName();
			}
		}

		return OTHER;
	}
}