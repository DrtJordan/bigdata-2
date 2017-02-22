package com.netease.weblogOffline.common;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpException;

import com.netease.weblogCommon.data.enums.SimpleDateFormatEnum;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogCommon.utils.HttpClientFactory;
import com.netease.weblogCommon.utils.JsonUtils;


public class getVidioToLacol {
	public static void main(String[] args) throws HttpException, ParseException {
		String yestoday = args[0];
		String nextday = DateUtils.getTheDayBefore(yestoday, -1);
		String localDir = args[1];
		long startTime = DateUtils.toLongTime(yestoday,
				SimpleDateFormatEnum.dateFormat.get());
		long endTime = DateUtils.toLongTime(nextday,
				SimpleDateFormatEnum.dateFormat.get());
		String url = "http://so.v.163.com/share/getvideosbytime.htm?start="
				+ startTime + "&end=" + endTime;
		List<String> list = HttpClientFactory.runGetMethod(url);
		String line = list.get(0);
		List<Map<String, String>> listmap = JsonUtils.json2List(line);
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(localDir));
			for (Map<String, String> map : listmap) {

				StringBuilder sb = new StringBuilder();
				sb.append(map.get("vid")).append("\t")
						.append(map.get("videoUrl")).append("\t")
						.append(map.get("title")).append("\t")
						.append(map.get("tag")).append("\t")
						.append(map.get("originalauthor")).append("\t")
						.append(map.get("uploader")).append("\t")
						.append(map.get("modifier")).append("\t")
						.append(map.get("source")).append("\t")
						.append(map.get("postid")).append("\t")
						.append(map.get("uploadtime")).append("\t")
						.append(map.get("executiveeditor")).append("\t")
						.append(map.get("lastmoditytime"));
				writer.write(sb.toString());
				writer.newLine();

			}
			writer.flush();
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
