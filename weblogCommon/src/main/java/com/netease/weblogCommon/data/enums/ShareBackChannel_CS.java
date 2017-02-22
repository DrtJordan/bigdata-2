package com.netease.weblogCommon.data.enums;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.netease.weblogCommon.utils.UrlUtils;

public enum ShareBackChannel_CS {
	qzone("qzone", "?snstj_qzone", "sns_qzone"),
	weibo("weibo","?snstj_weibo", "sns_weibo"),
	renren("renren", "?snstj_renren", "sns_renren"),
	weibo163("weibo163", "?snstj_163", "sns_163,share_weibo"),
	youdao("youdao", "?snstj_youdao", "sns_youdao"),
	yixin("yixin", "?snstj_yixin", "sns_yixin"),
	weixin("weixin", "?snstj_weixin", "sns_weixin"),
	lofter("lofter", "?snstj_lofter", "sns_lofter"),
	hot163("hot163", "?snstj_hot163", "sns_hot163");
		
	private String name;
	private List<String> shareChannels;
	private List<String> backChannels;

	private ShareBackChannel_CS(String name, String shareChannelStr, String backChannelStr) {
		this.name = name;
		if (StringUtils.isNotBlank(shareChannelStr)) {
			String[] sc = shareChannelStr.split(",");
			this.shareChannels = Arrays.asList(sc);
		}

		if (StringUtils.isNotBlank(backChannelStr)) {
			String[] bc = backChannelStr.split(",");
			this.backChannels = Arrays.asList(bc);
		}
	}

	public String getName() {
		return name;
	}


	/**
	 * url不是一个分享页,否则返回null; 
	 * url是一个分享页,返回分享渠道列表中的一个渠道;
	 * url是一个分享页,但非分享渠道列表中渠道，返回null.
	 * */
	public static String getShareChannel(String url) {
		if (UrlUtils.PATTERN_SHARE.matcher(url).matches()) {
			for (ShareBackChannel_CS one : ShareBackChannel_CS.values()) {
				for (String sc : one.shareChannels) {
					if (StringUtils.isNotBlank(sc) && url.contains(sc)) {
						return one.getName();
					}
				}
			}
		}

		return null;
	}

	/**
	 * url不是一个回流页,否则返回null;
	 * url是一个回流页,返回回流渠道列表中的一个渠道;
	 * url是一个回流页,但非回流渠道列表中渠道，返回null.
	 * */
	public static String getBackChannel(String url) {
		for (ShareBackChannel_CS one : ShareBackChannel_CS.values()) {
			for (String bs : one.backChannels) {
				if (StringUtils.isNotBlank(bs) && url.contains(bs)) {
					return one.getName();
				}
			}
		}

		return null;
	}
}