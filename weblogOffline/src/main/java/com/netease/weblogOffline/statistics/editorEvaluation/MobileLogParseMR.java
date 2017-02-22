package com.netease.weblogOffline.statistics.editorEvaluation;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hadoop.mapreduce.LzoTextInputFormat;
import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.annotation.NotCheckInputFile;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.utils.DirUtils;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.utils.HadoopUtils;
import com.netease.weblogOffline.utils.StringUtils.OriginalLogKeys;

@NotCheckInputFile
public class MobileLogParseMR extends MRJob {

	@Override
	public boolean init(String date) {
//		for(String p : DirUtils.getMobileHourPath(DirConstant.MOBILE_LOG, date)){
//			inputList.add(p);
//		}
		inputList.add(DirUtils.getMobilePartitionPath(DirConstant.MOBILE_LOG, date));
		outputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION + "mobileFormatLog/" + date);
		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");

		job.getConfiguration().set("dt", getParams().getDate());
		boolean b =false;
		for (String input : inputList) {
			Path path = new Path(input);
	            
		   b|=addInputPath(job, path, LzoTextInputFormat.class, MobileRawLogMapper.class);
		}
		
		if (!b){
			return 0;
		}
		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// reducer
		job.setReducerClass(MobileRawLogReducer.class);
		job.setNumReduceTasks(16);
		FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
		job.setOutputFormatClass(TextOutputFormat.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		if (!job.waitForCompletion(true)) {
			jobState = JobConstant.FAILED;
		}
		return jobState;
	}

	public static class MobileRawLogMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private Map<String, String> map = new HashMap<String, String>();
		private static DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		String[] eventnames = {"_pvX","_ivX","_vvX","_svX","SHARE_NEWS","$","^"};
		String[] appids = {"2x1kfBk63z","Gj9YiT","2S5Wcx","S4zbL7","IC8kYG","h4H6BN","J0ylV8","8Bn4W5","2lR24f","tI2rfh","23RfG1","JBfjmI","0V0Gvx","vNjbl9"};

		private boolean isValidSessionParam(String param) {
			return StringUtils.isNotBlank(param) && param.length() > 0 && param.length() <= 84;
		}

		private boolean isValidSessionParam(JSONObject jsonObject, String key) {
			if (!jsonObject.containsKey(key)) {
				return false;
			}
			return isValidSessionParam(jsonObject.getString(key));
		}
		public boolean isValidLogLine(String line) throws IOException, InterruptedException {
			if (line.length() == 0 || !line.contains("]")) {
				return false;
			}
			if (line.indexOf(OriginalLogKeys.JSON_IP_TAG) == -1) {
				return false;
			}
			if (line.indexOf(OriginalLogKeys.JSON_MESSAGE_TAG) == -1) {
				return false;
			}
			return true;
		}
		private boolean isValidParam(JSONObject jsonObject, String key) {
			if (!jsonObject.containsKey(key)) {
				return false;
			}
			return isValidParam(jsonObject.getString(key));
		}
		private boolean isValidParam(String param) {
			return !StringUtils.isBlank(param) && param.length() > 0
					&& param.length() <= 64;
		}


		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			try {
				if (!isValidLogLine(line)) {
					context.getCounter("MobileRawLogMapper", "logFormatError").increment(1);
					return;
				}
				int messageStart = line.indexOf(OriginalLogKeys.JSON_MESSAGE_TAG);
				String message = line.substring(messageStart + OriginalLogKeys.JSON_MESSAGE_TAG_LENGTH);
				JSONArray messageJa = JSONArray.fromObject(message);
				String appId = "";
				String uuid = "";
				String tstime = "";
				JSONObject devJo = null;
				Map<String, List<String>> output = new HashMap<String, List<String>>();
				Map<String, String> log = new HashMap<String, String>();

				for (Object o : messageJa) {
					JSONObject jo = (JSONObject) o;

					if (!isValidSessionParam(jo,OriginalLogKeys.JSON_MOBILE_SESSION)) {
						context.getCounter("MobileRawLogMapper", "sessionError").increment(1);
						continue;
					}

					String sessionId = jo.getString(OriginalLogKeys.JSON_MOBILE_SESSION);
					log.put("sessionId",sessionId);

					if(StringUtils.isBlank(appId) && jo.containsKey(OriginalLogKeys.JSON_DEVICE_APPID)){
						appId = jo.getString(OriginalLogKeys.JSON_DEVICE_APPID);
					}

					if(jo.containsKey(OriginalLogKeys.JSON_DEVICE)){
						devJo = jo.getJSONObject(OriginalLogKeys.JSON_DEVICE);
						context.getCounter("MobileRawLogMapper", "hasDeviceInfo").increment(1);
//						判断device是否合法
//						if (!deviceInfoValid(deviceInfo)) {
//							context.getCounter("MobileRawLogMapper", "DeviceInfoError").increment(1);
//							outputKey.set(sessionId);
//							outputValue.set(JsonUtils.toJson(devJo));
//							context.write(outputKey, outputValue);
//							continue;
//						}
						if(!devJo.containsKey("os")){
							context.getCounter("MobileRawLogMapper", "BlackOs").increment(1);
						}
						if(!devJo.containsKey("imei")){
							context.getCounter("MobileRawLogMapper", "BlackImei").increment(1);
						}
						if(!devJo.containsKey("ux")){
							context.getCounter("MobileRawLogMapper", "BlackUx").increment(1);
						}
						if(!devJo.containsKey("uv")){
							context.getCounter("MobileRawLogMapper", "BlackUv").increment(1);
						}
						if(!devJo.containsKey("u")){
							context.getCounter("MobileRawLogMapper", "BlackU").increment(1);
						}
						if(devJo.containsKey("os")){
							if("a".equals(devJo.getString("os"))&&devJo.containsKey("imei")&&devJo.containsKey("ux")){
								uuid = devJo.getString("imei")+"#"+devJo.getString("ux");
							}else {
								if(devJo.containsKey("uv")){
									uuid = devJo.getString("uv");
								}else if(devJo.containsKey("u")){
									uuid = devJo.getString("u");
								}else {
									uuid = devJo.getString("ux");
								}
							}
						}else {
							if(devJo.containsKey("uv")){
								uuid = devJo.getString("uv");
							}else if(devJo.containsKey("u")){
								uuid = devJo.getString("u");
							}else {
								uuid = devJo.getString("ux");
							}
						}
						log.put("uuid",uuid);
						context.getCounter("MobileRawLogMapper", "containsDeviceInfo").increment(1);
					}else {
						context.getCounter("MobileRawLogMapper", "notcontainsDeviceInfo").increment(1);
					}

					if(StringUtils.isBlank(appId) && devJo != null && devJo.containsKey(OriginalLogKeys.JSON_DEVICE_APPID)){
						appId = devJo.getString(OriginalLogKeys.JSON_DEVICE_APPID);
					}
					if (StringUtils.isBlank(appId)) {
						context.getCounter("MobileRawLogMapper", "blackAppIdLogLineCount").increment(1);
					}
					if (StringUtils.isBlank(uuid)) {
						context.getCounter("MobileRawLogMapper", "blackUUIDCount").increment(1);
					}
					log.put("appId",appId);

					if(java.util.Arrays.asList(appids).contains(appId)||StringUtils.isBlank(appId)){
						JSONArray events = jo.getJSONArray(OriginalLogKeys.JSON_EVENT);
						for(Object obj :events){
							JSONObject eventJo = (JSONObject) obj;
							// 判断是否包含n,t
							if ((!isValidParam(eventJo, OriginalLogKeys.JSON_EVENT_NAME) || !isValidParam(
									eventJo, OriginalLogKeys.JSON_EVENT_TIME))
									&& (!isValidParam(eventJo, OriginalLogKeys.JSON_EVENT_NAME) || !isValidParam( eventJo, OriginalLogKeys.JSON_EVENT_TIME_OLD))) {
								context.getCounter("MobileRawLogMapper", "eventParseError").increment(1);
								continue;
							}
							String eventname = eventJo .getString(OriginalLogKeys.JSON_EVENT_NAME);
							if(!java.util.Arrays.asList(eventnames).contains(eventname)){
								context.getCounter("MobileRawLogMapper", "notNeedEvent").increment(1);
								continue;
							}else if("_pvX".equals(eventname)){
								context.getCounter("MobileRawLogMapper", "_pvXCount").increment(1);
							}
							log.put("eventname",eventname);
							int acc = 0;
							if (eventJo.containsKey(OriginalLogKeys.JSON_EVENT_TC)) {
								acc = eventJo.getInt(OriginalLogKeys.JSON_EVENT_TC);
							}
							if (acc > 100 || acc < 0) {
								acc = 1;
							}
							log.put("acc",acc+"");

							if (eventJo.containsKey(OriginalLogKeys.JSON_EVENT_TIME)) {
								tstime = timeFormat.format(eventJo.getLong(OriginalLogKeys.JSON_EVENT_TIME));
							} else if (eventJo.containsKey(OriginalLogKeys.JSON_EVENT_TIME_OLD)) {
								tstime = timeFormat.format(eventJo.getLong(OriginalLogKeys.JSON_EVENT_TIME_OLD));
							}
							log.put("tstime",tstime);
							String eventtag = "";
							if (eventJo.containsKey(OriginalLogKeys.JSON_EVENT_TAG)) {
								eventtag = eventJo.getString(OriginalLogKeys.JSON_EVENT_TAG);
							} else {
								eventtag = eventJo.getString(OriginalLogKeys.JSON_EVENT_NAME);
							}
							if("_pvX".equals(eventname)){
								eventtag = eventtag.split("#")[0];
							}

							log.put("eventtag",eventtag);

							outputKey.set(sessionId);
							outputValue.set(JsonUtils.toJson(log));
							context.write(outputKey, outputValue);
						}
					}
				}
			} catch (Exception e) {
				context.getCounter("MobileRawLogMapper", "mapException").increment(1);
			}
		}
	}
	public static class MobileRawLogReducer extends Reducer<Text, Text, Text, NullWritable> {
		String uuid = "0";
		private NullWritable outputValue = NullWritable.get();
		private Text outputKey = new Text();
		List<String> logs;
		protected void setup(Context context) throws IOException, InterruptedException {
			logs = new ArrayList<String>();
		}
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			logs.clear();
			context.getCounter("MobileRawLogReducer", "sessionCount").increment(1);
			for(Text value : values){
				Map<String, String> valueMap = JsonUtils.json2StrMap(value.toString());
				logs.add(value.toString());
				if(valueMap.containsKey("uuid")){
					context.getCounter("MobileRawLogReducer", "hasuuidCount").increment(1);
					uuid = valueMap.get("uuid");
				}else {
					context.getCounter("MobileRawLogReducer", "nothasuuidCount").increment(1);
				}
			}
			for(String value : logs){
				Map<String, String> valueMap = JsonUtils.json2StrMap(value);
				valueMap.put("uuid",uuid);
				outputKey.set(JsonUtils.toJson(valueMap));
				context.write(outputKey, outputValue);
			}
		}
	}
}

