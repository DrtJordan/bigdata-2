package com.netease.weblogOffline.statistics.bigdatahouse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.HashMapStringIntegerWritable;
import com.netease.weblogOffline.data.HashMapStringStringWritable;
import com.netease.weblogOffline.data.StringStringWritable;
import com.netease.weblogOffline.data.ThreeStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;
public class GentieCountAndUvMR extends MRJob {
	
	@Override
	public boolean init(String date) {
		//
		inputList.add(DirConstant.GEN_TIE_INFO + date);
		outputList.add(DirConstant.HOUSE_DAILY_URL_GENTIE_TEMP + date);
		outputList.add(DirConstant.HOUSE_DAILY_URL_GENTIE + date);
		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;
	
			Job job3 = HadoopUtils.getJob(this.getClass(), this.getClass()
					.getName() + "_step1");

			// 跟帖
			MultipleInputs.addInputPath(job3, new Path(inputList.get(0)),
					TextInputFormat.class, GenTieMapper.class);

			// mapper
			job3.setMapOutputKeyClass(StringStringWritable.class);
			job3.setMapOutputValueClass(HashMapStringIntegerWritable.class);

			job3.setCombinerClass(GenTieTempReducer.class);
			// reducer
			job3.setReducerClass(GenTieTempReducer.class);
			job3.setNumReduceTasks(16);
			FileOutputFormat.setOutputPath(job3, new Path(outputList.get(0)));
			job3.setOutputFormatClass(SequenceFileOutputFormat.class);

			job3.setOutputKeyClass(StringStringWritable.class);
			job3.setOutputValueClass(HashMapStringIntegerWritable.class);

			if (!job3.waitForCompletion(true)) {
				jobState = JobConstant.FAILED;
			}

			Job job4 = HadoopUtils.getJob(this.getClass(), this.getClass()
					.getName() + "_step2");

			MultipleInputs.addInputPath(job4, new Path(outputList.get(0)),
					SequenceFileInputFormat.class, GenTieTempMapper.class);

			// mapper
			job4.setMapOutputKeyClass(Text.class);
			job4.setMapOutputValueClass(HashMapStringIntegerWritable.class);

			// reducer
			job4.setReducerClass(GenTieReducer.class);
			job4.setNumReduceTasks(16);
			FileOutputFormat.setOutputPath(job4, new Path(outputList.get(1)));
			job4.setOutputFormatClass(SequenceFileOutputFormat.class);

			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(HashMapStringStringWritable.class);

			if (!job4.waitForCompletion(true)) {
				jobState = JobConstant.FAILED;
				
			}
			
			return jobState;
		
	}


	public static class GenTieMapper
			extends
			Mapper<LongWritable, Text, StringStringWritable, HashMapStringIntegerWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {


				StringStringWritable puress = new StringStringWritable();

				HashMapStringIntegerWritable pureiotk = new HashMapStringIntegerWritable();

				HashMap<String, String> hm = GenTieUtils.logParser(value);
				String url = hm.get("url");
				String uuid = hm.get("uuid");
				String source = DailyUrlInfoUtils.getGentieSource(hm
						.get("gentiesource"));

	

				pureiotk.getHm().put("pure_gentieCount", 1);
				pureiotk.getHm().put("pure_" + source + "_gentieCount", 1);
				String pureUrl = UrlUtils.getOriginalUrl(url);
				puress.setFirst(pureUrl);
				puress.setSecond(uuid);
				context.write(puress, pureiotk);

			} catch (Exception e) {
				context.getCounter("GenTieMapper", "mapException").increment(1);
			}
		}
	}

	public static class GenTieTempReducer
			extends
			Reducer<StringStringWritable, HashMapStringIntegerWritable, StringStringWritable, HashMapStringIntegerWritable> {

		@Override
		protected void reduce(StringStringWritable key,
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

	public static class GenTieTempMapper
			extends
			Mapper<StringStringWritable, HashMapStringIntegerWritable, Text, HashMapStringIntegerWritable> {

		@Override
		public void map(StringStringWritable key,
				HashMapStringIntegerWritable value, Context context)
				throws IOException, InterruptedException {
			Text ss = new Text();
			ss.set(key.getFirst());// url

			context.write(ss, value);
		}
	}

	public static class GenTieReducer
			extends
			Reducer<Text, HashMapStringIntegerWritable, Text, HashMapStringStringWritable> {

		@Override
		protected void reduce(Text key,
				Iterable<HashMapStringIntegerWritable> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> hm = new HashMap<String, Integer>();
			ArrayList<String> al = new ArrayList<String>();

			for (HashMapStringIntegerWritable val : values) {
				al.clear();
				for (String s : val.getHm().keySet()) {
					if (hm.get(s) != null) {
						hm.put(s, new Integer(hm.get(s) + val.getHm().get(s)));
					} else {
						hm.put(s, new Integer(val.getHm().get(s)));
					}
					String[] str = s.split("_");
					if (str.length>=2){
						String source = str[str.length - 2];
						if ((!al.contains(source)) && (!source.equals("pure"))) {
							al.add(source);
						}
					}
			

				}

					for (String s : al) {
						if (hm.get("pure_" + s + "_gentieUserCount") != null) {
							hm.put("pure_" + s + "_gentieUserCount",
									hm.get("pure_" + s + "_gentieUserCount") + 1);
							hm.put( s + "_gentieUserCount",
							hm.get( s + "_gentieUserCount") + 1);
							
						} else {
							hm.put("pure_" + s + "_gentieUserCount", 1);
							hm.put( s + "_gentieUserCount", 1);
						}
					}
					if (hm.get("pure_gentieUserCount") != null) {
						hm.put("pure_gentieUserCount",
								hm.get("pure_gentieUserCount") + 1);
						hm.put("gentieUserCount",
						hm.get("gentieUserCount") + 1);
					} else {
						hm.put("pure_gentieUserCount", 1);
						hm.put("gentieUserCount", 1);
					}
				

			}
			HashMapStringStringWritable hmss = new HashMapStringStringWritable();
			for (String s : DailyUrlInfoUtils.getGentiecolumns()) {
				hmss.getHm().put(
						s,
						hm.get(s) == null ? DailyUrlInfoUtils.defNullInt
								: String.valueOf(hm.get(s)));
			}
			context.write(key, hmss); // gentie 的所有指标
		}

	}
      
}
