package com.netease.weblogOffline.statistics.bigdatahouse;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
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
import com.netease.weblogCommon.utils.TextUtils;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.HashMapStringStringWritable;
import com.netease.weblogOffline.data.StringLongSemiHash;
import com.netease.weblogOffline.utils.HadoopUtils;
public class DailyUrlInfoVector2MR extends MRJob {

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
		

			Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "_step1");
			
			MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, DailyUrlInfoMapper_1.class);//url pv uv share back
			
			MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, DailyUrlInfoMapper_0.class);  //gentie count and user_count 
			MultipleInputs.addInputPath(job, new Path(inputList.get(2)), SequenceFileInputFormat.class, DailyUrlInfoMapper_0.class); //是否原创
			MultipleInputs.addInputPath(job, new Path(inputList.get(3)), SequenceFileInputFormat.class, DailyUrlInfoMapper_0.class);//文章来源

			// mapper
			job.setMapOutputKeyClass(StringLongSemiHash.class);
			job.setMapOutputValueClass(HashMapStringStringWritable.class);

			// reducer
			job.setReducerClass(DailyUrlInfoReducer.class);
			job.setNumReduceTasks(16);
			
			FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(HashMapStringStringWritable.class);
			
			if (!job.waitForCompletion(true)) {
				jobState = JobConstant.FAILED;
			}
			return jobState;
		
	}

	public static class DailyUrlInfoMapper_0 extends Mapper<Text, HashMapStringStringWritable, StringLongSemiHash, HashMapStringStringWritable> {
		private StringLongSemiHash outputKey = new StringLongSemiHash(null, 0);
		@Override
		public void map(Text key, HashMapStringStringWritable value, Context context) throws IOException, InterruptedException {
			outputKey.setFirst(key.toString());
			context.write(outputKey, value);
		}
	}
	
	public static class DailyUrlInfoMapper_1 extends Mapper<Text, HashMapStringStringWritable, StringLongSemiHash, HashMapStringStringWritable> {
		private StringLongSemiHash outputKey = new StringLongSemiHash(null, 1);
		@Override
		public void map(Text key, HashMapStringStringWritable value, Context context) throws IOException, InterruptedException {
			outputKey.setFirst(key.toString());
			context.write(outputKey, value);
		}
	}
		
	public static class DailyUrlInfoReducer extends Reducer<StringLongSemiHash, HashMapStringStringWritable, Text, HashMapStringStringWritable> {
		
		private MultipleOutputs<Text,HashMapStringStringWritable> mos;

		
		private String pureUrl = null;
		private boolean outputCommonInfo = true;
		private HashMap<String, String> commonInfoMap = new HashMap<String, String>();

		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, HashMapStringStringWritable>(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if(outputCommonInfo){
				/**输出commonInfoMap*/
				HashMapStringStringWritable pureUrlOutputCommonInfoMap = new HashMapStringStringWritable();
				
				
					pureUrlOutputCommonInfoMap.setHm( preparePureUrlResult(commonInfoMap,pureUrl));
					try {
						URL url = new URL(TextUtils.notNullStr(pureUrlOutputCommonInfoMap.getHm().get("pure_url"),DailyUrlInfoUtils.defNullStr));
		                mos.write(new Text(pureUrlOutputCommonInfoMap.getHm().get("pure_url")), pureUrlOutputCommonInfoMap, "pureurl/part");
				    	}catch (Exception e) {}
				
				
			}
			mos.close();
			super.cleanup(context);
		}
		
		@Override
		protected void reduce(StringLongSemiHash key, Iterable<HashMapStringStringWritable> values, Context context) throws IOException, InterruptedException {
			
			if(pureUrl == null){
				pureUrl = key.getFirst();
			}
			
			if(key.getSecond() == 0){//info
				if(!key.getFirst().equals(pureUrl)){//换了一个pureUrl
					if(outputCommonInfo){
						/**输出commonInfoMap*/
	
						HashMapStringStringWritable pureUrlOutputCommonInfoMap = new HashMapStringStringWritable();
							pureUrlOutputCommonInfoMap.setHm( preparePureUrlResult(commonInfoMap,pureUrl));
							try {
								URL url = new URL(TextUtils.notNullStr(pureUrlOutputCommonInfoMap.getHm().get("pure_url"),DailyUrlInfoUtils.defNullStr));
				                mos.write(new Text(pureUrlOutputCommonInfoMap.getHm().get("pure_url")), pureUrlOutputCommonInfoMap, "pureurl/part");
						    	}catch (Exception e) {}
				
						
						
					}
					
					outputCommonInfo = true;
					pureUrl = key.getFirst();
					commonInfoMap.clear();
				}
				
				for(HashMapStringStringWritable val : values){
					commonInfoMap.putAll(val.getHm());
				}
			}else{//data
				if(key.getFirst().equals(pureUrl)){
					/**拼接value和commonInfoMap，输出*/
					HashMapStringStringWritable valueCommonInfoMap = new HashMapStringStringWritable();
					valueCommonInfoMap.getHm().putAll(commonInfoMap);
					for(HashMapStringStringWritable val : values){
						valueCommonInfoMap.getHm().putAll(val.getHm());
					}
					
					
					if (valueCommonInfoMap.getHm().get("url")!=null){
						valueCommonInfoMap.setHm(prepareUrlResult(valueCommonInfoMap.getHm(),pureUrl));
						try {
							URL url = new URL(TextUtils.notNullStr(valueCommonInfoMap.getHm().get("url"),DailyUrlInfoUtils.defNullStr));
			                mos.write(new Text(valueCommonInfoMap.getHm().get("url")), valueCommonInfoMap, "url/part");
					    	}catch (Exception e) {}
					}else {
						valueCommonInfoMap.setHm(preparePureUrlResult(valueCommonInfoMap.getHm(),pureUrl));
						try {
							URL url = new URL(TextUtils.notNullStr(valueCommonInfoMap.getHm().get("pure_url"),DailyUrlInfoUtils.defNullStr));
			                mos.write(new Text(valueCommonInfoMap.getHm().get("pure_url")), valueCommonInfoMap, "pureurl/part");
					    	}catch (Exception e) {}
					}
					
					
					
					outputCommonInfo = false;
				}else {
					/**不拼接commonInfoMap,输出value*/
					
					HashMapStringStringWritable valueInfoMap = new HashMapStringStringWritable();
					for(HashMapStringStringWritable val : values){
						valueInfoMap.getHm().putAll(val.getHm());
					}
					if (valueInfoMap.getHm().get("url")!=null){
						String pureUrl = UrlUtils.getOriginalUrl(valueInfoMap.getHm().get("url"));
						valueInfoMap.setHm(prepareUrlResult(valueInfoMap.getHm(),pureUrl));
						try {
							URL url = new URL(TextUtils.notNullStr(valueInfoMap.getHm().get("url"),DailyUrlInfoUtils.defNullStr));
			                mos.write(new Text(valueInfoMap.getHm().get("url")), valueInfoMap, "url/part");
					    	}catch (Exception e) {}
					}else {
						valueInfoMap.setHm(preparePureUrlResult(valueInfoMap.getHm(),valueInfoMap.getHm().get("pure_url")));
						try {
							URL url = new URL(TextUtils.notNullStr(valueInfoMap.getHm().get("pure_url"),DailyUrlInfoUtils.defNullStr));
			                mos.write(new Text(valueInfoMap.getHm().get("pure_url")), valueInfoMap, "pureurl/part");
					    	}catch (Exception e) {}
					}
					
					
				}
			}

			
		}
		
		private   HashMap<String, String> prepareUrlResult(HashMap<String, String> hm,String pureUrl){
			
			
			HashMap<String, String>  urlHm =  new HashMap<String, String> ();
			HashMap<String, String>  urlHmResult =  new HashMap<String, String> ();
			urlHm.putAll(hm);
			urlHm.put("pure_url", pureUrl);
			urlHm.put("type", NeteaseContentType.getTypeName(urlHm.get("url")));
			urlHm.put("mediasource", hm.get("pure_mediasource")==null? DailyUrlInfoUtils.defNullStr:urlHm.get("pure_mediasource"));
			urlHm.put("isyc", hm.get("pure_isyc")==null?DailyUrlInfoUtils.defNullStr:urlHm.get("pure_isyc"));
	
	
			for (String s : DailyUrlInfoUtils.getUrlcolumns()) {
				urlHmResult.put(s, urlHm.get(s) == null ? DailyUrlInfoUtils.defNullInt : urlHm.get(s));
			}
			urlHmResult.put("pure_url", pureUrl);
			return  urlHmResult;
		}
		
		private   HashMap<String, String> preparePureUrlResult(HashMap<String, String> hm,String pureUrl){
			HashMap<String, String>  pureUrlHm =  new HashMap<String, String> ();
			HashMap<String, String>  pureUrlHmResult =  new HashMap<String, String> ();
			pureUrlHm.putAll(hm);
			pureUrlHm.put("pure_url", pureUrl);
			pureUrlHm.put("pure_type", NeteaseContentType.getTypeName(pureUrl));
			pureUrlHm.put("pure_mediasource", hm.get("pure_mediasource")==null? DailyUrlInfoUtils.defNullStr:pureUrlHm.get("pure_mediasource"));
			pureUrlHm.put("pure_isyc", hm.get("pure_isyc")==null?DailyUrlInfoUtils.defNullStr:pureUrlHm.get("pure_isyc"));
			for (String s :DailyUrlInfoUtils.getPureurlcolumns()){
				pureUrlHmResult.put(s, pureUrlHm.get(s) == null ? DailyUrlInfoUtils.defNullInt : pureUrlHm.get(s));
		    }
			
			return  pureUrlHmResult;
		}
		
		
		
	}
      
}
