package com.netease.weblogOffline.statistics.bigdatahouse;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogCommon.data.enums.ShareBackChannel_CS;
import com.netease.weblogCommon.utils.TextUtils;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.HashMapStringIntegerWritable;
import com.netease.weblogOffline.data.HashMapStringStringWritable;
import com.netease.weblogOffline.data.StringStringWritable;
import com.netease.weblogOffline.data.ThreeStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;
public class DailyUrlInfoVectorMR extends MRJob {

	@Override
	public boolean init(String date) {
		//
 		inputList.add(DirConstant.HOUSE_DAILY_URL_PVUV_SHAREBACK + date);
 		inputList.add(DirConstant.HOUSE_DAILY_URL_GENTIE + date);
 		inputList.add(DirConstant.HOUSE_DAILY_URL_YC + date);
 		inputList.add(DirConstant.HOUSE_DAILY_URL_MEDIA + date);
		outputList.add(DirConstant.HOUSE_DAILY_URL_INFO + date);
		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;
		
	
	
			

			Job job7 = HadoopUtils.getJob(this.getClass(), this.getClass()
					.getName() + "_step1");
			
			MultipleInputs.addInputPath(job7, new Path(inputList.get(0)),
					SequenceFileInputFormat.class, DailyUrlInfoMapper.class);//url pv uv share back 
			
			MultipleInputs.addInputPath(job7, new Path(inputList.get(1)),
					SequenceFileInputFormat.class, DailyUrlInfoMapper.class);  //gentie count and user_count 
			
			MultipleInputs.addInputPath(job7, new Path(inputList.get(2)),
					SequenceFileInputFormat.class, DailyUrlInfoMapper.class); //是否原创
			
			MultipleInputs.addInputPath(job7, new Path(inputList.get(3)),
					SequenceFileInputFormat.class, DailyUrlInfoMapper.class);//文章来源

			// mapper
			job7.setMapOutputKeyClass(Text.class);
			job7.setMapOutputValueClass(HashMapStringStringWritable.class);

			// reducer
			job7.setReducerClass(DailyUrlInfoReducer.class);
			job7.setNumReduceTasks(150);
			job7.getConfiguration().set("mapreduce.reduce.memory.mb", "8096");
			FileOutputFormat.setOutputPath(job7, new Path(outputList.get(0)));
			job7.setOutputFormatClass(SequenceFileOutputFormat.class);

			job7.setOutputKeyClass(Text.class);
			job7.setOutputValueClass(HashMapStringStringWritable.class);
			
//	        MultipleOutputs.addNamedOutput(job7, "pureurl", SequenceFileOutputFormat.class, Text.class, HashMapStringStringWritable.class);
//	        MultipleOutputs.addNamedOutput(job7, "url", SequenceFileOutputFormat.class, Text.class, HashMapStringStringWritable.class);

			if (!job7.waitForCompletion(true)) {
				jobState = JobConstant.FAILED;
			}
			return jobState;
		
	}

	public static class ZylogMapper
			extends
			Mapper<Text, HashMapStringStringWritable, ThreeStringWritable, HashMapStringIntegerWritable> {

		@Override
		public void map(Text key, HashMapStringStringWritable value, Context context)
				throws IOException, InterruptedException {

			try {

				HashMapStringIntegerWritable hmss = new HashMapStringIntegerWritable();
				HashMapStringIntegerWritable purehmss = new HashMapStringIntegerWritable();
		
				String url = value.getHm().get("url");
				String uuid =  value.getHm().get("uuid");
				// String type = NeteaseContentType.getTypeName(url);

				ThreeStringWritable ss = new ThreeStringWritable();
				ss.setfirst(url);
				ss.setsecond(uuid);
				ss.setthird("o_url");

				hmss.getHm().put("pv_pc", 1);

				ThreeStringWritable puress = new ThreeStringWritable();
				String pureUrl = UrlUtils.getOriginalUrl(url);
				puress.setfirst(pureUrl);
				puress.setsecond(uuid);
				puress.setthird("p_url");

				purehmss.getHm().put("pure_pv_pc", 1);

				String suffix = null;

				if (null != (suffix = ShareBackChannel_CS.getShareChannel(url))) {
					hmss.getHm().put(suffix + "_shareCount_pc", 1);
					purehmss.getHm().put("pure_" + suffix + "_shareCount_pc",
							1);
					hmss.getHm().put("shareCount_pc", 1);
					purehmss.getHm().put("pure_shareCount_pc", 1);
					context.write(ss, hmss);
					context.write(puress, purehmss);

				} else if (null != (suffix = ShareBackChannel_CS
						.getBackChannel(url))) {
					hmss.getHm().put(suffix + "_backCount_pc", 1);
					purehmss.getHm()
							.put("pure_"+suffix + "_backCount_pc", 1);
					hmss.getHm().put("backCount_pc", 1);
					purehmss.getHm().put("pure_backCount_pc", 1);
					context.write(ss, hmss);
					context.write(puress, purehmss);

				} else {
					context.write(ss, hmss);
					context.write(puress, purehmss);
				}

			} catch (Exception e) {
				context.getCounter("LogMapper", "mapException").increment(1);
			}
		}
	}

	public static class PvUvShareBackMapper
			extends
			Mapper<ThreeStringWritable, HashMapStringIntegerWritable, StringStringWritable, HashMapStringIntegerWritable> {

		@Override
		public void map(ThreeStringWritable key,
				HashMapStringIntegerWritable value, Context context)
				throws IOException, InterruptedException {
			StringStringWritable ss = new StringStringWritable();
			ss.setFirst(key.getfirst());// url
			ss.setSecond(key.getthird());// pure or origin flag

			context.write(ss, value);
		}
	}

	public static class GenTieMapper
			extends
			Mapper<LongWritable, Text, ThreeStringWritable, HashMapStringIntegerWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				ThreeStringWritable ss = new ThreeStringWritable();

				ThreeStringWritable puress = new ThreeStringWritable();

				HashMapStringIntegerWritable iotk = new HashMapStringIntegerWritable();
				HashMapStringIntegerWritable pureiotk = new HashMapStringIntegerWritable();

				HashMap<String, String> hm = GenTieUtils.logParser(value);
				String url = hm.get("url");
				String uuid = hm.get("uuid");
				String source = DailyUrlInfoUtils.getGentieSource(hm
						.get("gentiesource"));

				iotk.getHm().put("gentieCount", 1);
				iotk.getHm().put(source + "_gentieCount", 1);

				ss.setfirst(url);
				ss.setsecond(uuid);
				ss.setthird("o_url");

				pureiotk.getHm().put("pure_gentieCount", 1);
				pureiotk.getHm().put("pure_" + source + "_gentieCount", 1);
				String pureUrl = UrlUtils.getOriginalUrl(url);
				puress.setfirst(pureUrl);
				puress.setsecond(uuid);
				puress.setthird("p_url");

				context.write(ss, iotk);
				context.write(puress, pureiotk);

			} catch (Exception e) {
				context.getCounter("GenTieMapper", "mapException").increment(1);
			}
		}
	}

	public static class GenTieTempMapper
			extends
			Mapper<ThreeStringWritable, HashMapStringIntegerWritable, StringStringWritable, HashMapStringIntegerWritable> {

		@Override
		public void map(ThreeStringWritable key,
				HashMapStringIntegerWritable value, Context context)
				throws IOException, InterruptedException {
			StringStringWritable ss = new StringStringWritable();
			ss.setFirst(key.getfirst());// url
			ss.setSecond(key.getthird());// pure or origin flag

			context.write(ss, value);
		}
	}

	public static class CmsYcMapper
			extends
			Mapper<Text, HashMapStringStringWritable, Text, HashMapStringStringWritable> {

		@Override
		public void map(Text key, HashMapStringStringWritable value,
				Context context) throws IOException, InterruptedException {
			HashMapStringStringWritable hmss = new HashMapStringStringWritable();
			hmss.getHm().put("pure_isyc", "true");
			context.write(key, hmss);
		}
	}

	public static class MediaSourceMapper
			extends
			Mapper<Text, HashMapStringStringWritable, Text, HashMapStringStringWritable> {

		@Override
		public void map(Text key, HashMapStringStringWritable value,
				Context context) throws IOException, InterruptedException {
			HashMapStringStringWritable hmss = new HashMapStringStringWritable();
			hmss.getHm().put("pure_mediasource", value.getHm().get("mediasource"));
			context.write(key, hmss);
		}
	}

	public static class DailyUrlInfoMapper
	extends
	Mapper<Text, HashMapStringStringWritable, Text, HashMapStringStringWritable> {
		
		@Override
		public void map(Text key, HashMapStringStringWritable value,
				Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
		}
		
	public static class DailyUrlInfoReducer extends Reducer<Text, HashMapStringStringWritable, Text, HashMapStringStringWritable> {
		
		private MultipleOutputs<Text,HashMapStringStringWritable> mos;
		
		private Map<String,String> commonInfo = new HashMap<String,String>();
		
		private ArrayList<HashMap<String,String>> urlDataList = new ArrayList<HashMap<String,String>>();
		
		private HashMapStringStringWritable  pureurlhmss = new HashMapStringStringWritable();

		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, HashMapStringStringWritable>(context);
		}

		
		@Override
		protected void reduce(Text key, Iterable<HashMapStringStringWritable> values, Context context) throws IOException, InterruptedException {
			commonInfo.clear();
			urlDataList.clear();
			pureurlhmss.getHm().clear();
			
			for (HashMapStringStringWritable val : values) {

				if (val.getHm().get("url")!=null){//原始url
					urlDataList.add(new HashMap<String,String>(val.getHm()));
				}else {//pure url
	               for (String s : val.getHm().keySet()){
	            	   if (commonInfo.get(s)!=null){
	            		   int i = Integer.parseInt(commonInfo.get(s)) + Integer.parseInt(val.getHm().get(s));
	            		   commonInfo.put(s, String.valueOf(i));   
	            	   }else {
	            		   commonInfo.put(s, new String(val.getHm().get(s)));   
	            	   }
	               }

				}
			}
			
			commonInfo.put("pure_url", key.toString());
			commonInfo.put("pure_type", NeteaseContentType.getTypeName(key.toString()));
			commonInfo.put("pure_mediasource", commonInfo.get("pure_mediasource")==null? DailyUrlInfoUtils.defNullStr:commonInfo.get("pure_mediasource"));
			commonInfo.put("pure_isyc", commonInfo.get("pure_isyc")==null?DailyUrlInfoUtils.defNullStr:commonInfo.get("pure_isyc"));
			
			for (String s :DailyUrlInfoUtils.getSharebackcolumns()){
				if (s.startsWith("pure_")){
					commonInfo.put(s, commonInfo.get(s)==null? DailyUrlInfoUtils.defNullInt: commonInfo.get(s));
				}
			}
			for (String s :DailyUrlInfoUtils.getPvuvcolumns()){
				if (s.startsWith("pure_")) {
					commonInfo.put(s, commonInfo.get(s) == null ? DailyUrlInfoUtils.defNullInt : commonInfo.get(s));
				}
			}
			
			for (String s :DailyUrlInfoUtils.getGentiecolumns()){
				commonInfo.put(s, commonInfo.get(s)==null? DailyUrlInfoUtils.defNullInt: commonInfo.get(s));
			}
		
			if (urlDataList.size() > 0) {
				for (HashMap<String, String> temp : urlDataList) {
					try {
						URL url = new URL(TextUtils.notNullStr(temp.get("url"), DailyUrlInfoUtils.defNullStr));

						HashMapStringStringWritable urlhmss = new HashMapStringStringWritable();
						temp.put("type", NeteaseContentType.getTypeName(temp.get("url")));
						temp.put("isyc", commonInfo.get("pure_isyc"));
						temp.put("mediasource", commonInfo.get("pure_mediasource"));

						for (String s : DailyUrlInfoUtils.getGentiecolumns()) {
							if (!s.startsWith("pure_")) {
								temp.put(s, new String(commonInfo.get(s)));
							}
						}

						for (String s : DailyUrlInfoUtils.getUrlcolumns()) {
							urlhmss.getHm().put(s, temp.get(s) == null ? DailyUrlInfoUtils.defNullInt : temp.get(s));
						}

						mos.write(new Text(urlhmss.getHm().get("url")), urlhmss, "url/part");
					} catch (Exception e) {}
				}
			}
			

			for (String s :DailyUrlInfoUtils.getPureurlcolumns()){
				pureurlhmss.getHm().put(s, commonInfo.get(s));
			}
			
			try {
				URL url = new URL(TextUtils.notNullStr(pureurlhmss.getHm().get("pure_url"),DailyUrlInfoUtils.defNullStr));
                mos.write(new Text(pureurlhmss.getHm().get("pure_url")), pureurlhmss, "pureurl/part");
			}catch (Exception e) {}
			
		}


		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
			super.cleanup(context);
		}
	}
      
}
