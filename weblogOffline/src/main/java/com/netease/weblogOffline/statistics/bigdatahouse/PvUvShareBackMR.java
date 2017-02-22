package com.netease.weblogOffline.statistics.bigdatahouse;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.ShareBackChannel_CS;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.HashMapStringIntegerWritable;
import com.netease.weblogOffline.data.HashMapStringStringWritable;
import com.netease.weblogOffline.data.StringStringWritable;
import com.netease.weblogOffline.data.ThreeStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;
public class PvUvShareBackMR extends MRJob {

	@Override
	public boolean init(String date) {
		//
    	   inputList.add(DirConstant.HOUSE_ZYLOG_MAP + date);
    	   outputList.add(DirConstant.HOUSE_DAILY_URL_PVUV_SHAREBACK_TEMP + date);
   		   outputList.add(DirConstant.HOUSE_DAILY_URL_PVUV_SHAREBACK + date);
		   return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

        	   
 
        	   Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass()
       				.getName() + "_step1");
               
       		// 章鱼
       		MultipleInputs.addInputPath(job1, new Path(inputList.get(0)),
       				SequenceFileInputFormat.class, ZylogMapper.class);

       		// mapper
       		job1.setMapOutputKeyClass(ThreeStringWritable.class);
       		job1.setMapOutputValueClass(HashMapStringIntegerWritable.class);

       		// combiner
       		job1.setCombinerClass(PvUvShareBackTempReducer.class);

       		// reducer
       		job1.setReducerClass(PvUvShareBackTempReducer.class);
       		job1.setNumReduceTasks(16);
       		FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
       		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

       		job1.setOutputKeyClass(ThreeStringWritable.class);
       		job1.setOutputValueClass(HashMapStringIntegerWritable.class);

       		if (!job1.waitForCompletion(true)) {
       			jobState = JobConstant.FAILED;
       		}

       		Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass()
       				.getName() + "_step2");

       		MultipleInputs.addInputPath(job2, new Path(outputList.get(0)),
       				SequenceFileInputFormat.class, PvUvShareBackMapper.class);

       		// mapper
       		job2.setMapOutputKeyClass(StringStringWritable.class);
       		job2.setMapOutputValueClass(HashMapStringIntegerWritable.class);

       		// reducer
       		job2.setReducerClass(PvUvShareBackReducer.class);
       		job2.setNumReduceTasks(16);
       		FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
       		job2.setOutputFormatClass(SequenceFileOutputFormat.class);

       		job2.setOutputKeyClass(Text.class);
       		job2.setOutputValueClass(HashMapStringStringWritable.class);

       		if (!job2.waitForCompletion(true)) {
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

	public static class PvUvShareBackTempReducer
			extends
			Reducer<ThreeStringWritable, HashMapStringIntegerWritable, ThreeStringWritable, HashMapStringIntegerWritable> {

		@Override
		protected void reduce(ThreeStringWritable key,
				Iterable<HashMapStringIntegerWritable> values, Context context)
				throws IOException, InterruptedException {
			HashMapStringIntegerWritable hmss = new HashMapStringIntegerWritable();
			for (HashMapStringIntegerWritable val : values) {
				for (String s : val.getHm().keySet()) {
					if (hmss.getHm().get(s) == null) {
						hmss.getHm().put(s, new Integer(val.getHm().get(s)));
					} else {
						hmss.getHm().put(s,
								new Integer(val.getHm().get(s) + hmss.getHm().get(s)));
					}
				}

			}
			context.write(key, hmss);
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

	public static class PvUvShareBackReducer
			extends
			Reducer<StringStringWritable, HashMapStringIntegerWritable, Text, HashMapStringStringWritable> {

		@Override
		protected void reduce(StringStringWritable key,
				Iterable<HashMapStringIntegerWritable> values, Context context)
				throws IOException, InterruptedException {

			HashMap<String, Integer> hm = new HashMap<String, Integer>();
			HashMapStringStringWritable hmss = new HashMapStringStringWritable();

			int uv = 0;
			for (HashMapStringIntegerWritable val : values) {
				for (String s : val.getHm().keySet()) {
					if (hm.get(s) == null) {
						hm.put(s, new Integer(val.getHm().get(s)));
					} else {
						hm.put(s, new Integer(val.getHm().get(s) + hm.get(s)));
					}
				}
				uv++;
			}

			if (key.getSecond().equals("o_url")) {
				hm.put("uv_pc", uv);
				hmss.getHm().put("url", key.getFirst());
				for (String s : hm.keySet()) {
					hmss.getHm().put(s, String.valueOf(hm.get(s)));
				}
				String pureUrl = UrlUtils.getOriginalUrl(key.getFirst());
				context.write(new Text(pureUrl), hmss);
			} else if (key.getSecond().equals("p_url")) {
				hm.put("pure_uv_pc", uv);
				hmss.getHm().put("pure_url", key.getFirst());
				for (String s : hm.keySet()) {
					hmss.getHm().put(s, String.valueOf(hm.get(s)));
				}
				context.write(new Text(key.getFirst()), hmss);
			}
			// hmss 里有url pv uv and share back zhibiao or pure_url pure_pv
			// pure_uv and share back zhibiao
		}
	}

      
}
