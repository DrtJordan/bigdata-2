package com.netease.weblogCommon.logparsers;

import java.util.Map;

/**
 * 为不同产品的日志解析提供统一接口
 * */
public interface LogParser {
	public Map<String, String> parse(String line);
}
