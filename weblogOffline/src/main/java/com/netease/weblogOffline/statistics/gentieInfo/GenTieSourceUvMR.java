package com.netease.weblogOffline.statistics.gentieInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 
 * */
public class GenTieSourceUvMR extends MRJob {
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.GEN_TIE_INFO + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_DIR+"result_other/GenTieSourceUv/" + date);
		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());

		MultipleInputs.addInputPath(job, new Path(inputList.get(0)),
				TextInputFormat.class, GenTieInfoMapper.class);
		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reducer
		job.setReducerClass(DistinctReducer.class);
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if (!job.waitForCompletion(true)) {
			jobState = JobConstant.FAILED;
		}

		return jobState;
	}

	public static class GenTieInfoMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private Text outputKey = new Text();
		private Map<String, String> map = new HashMap<String, String>();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				map.clear();

				// url,pdocid,docid,发帖用户id,跟帖时间，跟帖id,ip,source
				String[] strs = value.toString().split(",");
				String url = strs[0];
				String uid = strs[3];
				String source = strs[7];


				if (source.equals("ph")) {
					map.put("uid", uid);
					map.put("source", "ph");
					
				} else if (source.equals("wb")) {
					map.put("uid", uid);
					map.put("source", "wb");
				} else if (source.equals("zq")) {
					map.put("uid", uid);
					map.put("source", "zq");
				} else if (source.equals("3g")) {
					map.put("uid", uid);
					map.put("source", "3g");
				} else {
					map.put("uid", uid);
					map.put("source", "other");
				}
	
				outputKey.set(JsonUtils.toJson(map));
				context.write(outputKey, new IntWritable(1));
			} catch (Exception e) {
				context.getCounter("GenTieInfoMapper", "mapException")
						.increment(1);
			}
		}
	}

	public static class DistinctReducer extends
			Reducer<Text, IntWritable, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private HashMap<String, Integer> hm = new HashMap<String, Integer>();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			Map<String, String> map = JsonUtils.json2Map(key.toString());
			String source = map.get("source");
			if (hm.get(source)==null){
				hm.put(source, 1);
			}else {
				hm.put(source, hm.get(source)+1);
			}
			
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			String[] vaildzhibiao = new String[] {
					"ph", "wb", "zq", "3g", "other" };
			
	
				StringBuilder sb = new StringBuilder();

				for (String zhibiao : vaildzhibiao) {
					sb.append(
							hm.get(zhibiao) == null ? 0 : hm.get(zhibiao)).append("\t");
				}

				outputKey.set("uvDistribute");
				outputValue.set(sb.toString().trim());
				context.write(outputKey, outputValue);


		}

	}

}
