package com.netease.weblogOffline.statistics.gentieInfo;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.HashMapStringIntegerWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 
 * */
public class GenTieChannelSourceCountUvMR extends MRJob {
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.GEN_TIE_INFO + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_DIR+"result_other/GenTieChannelSourceCountUv/" + date);
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
		job.setMapOutputValueClass(HashMapStringIntegerWritable.class);

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
			Mapper<LongWritable, Text, Text, HashMapStringIntegerWritable> {

		private Text outputKey = new Text();
		private Map<String, String> map = new HashMap<String, String>();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				map.clear();
				HashMapStringIntegerWritable iotk = new HashMapStringIntegerWritable();

				// url,pdocid,docid,发帖用户id,跟帖时间，跟帖id,ip,source
				String[] strs = value.toString().split(",");
				String url = strs[0];
				String uid = strs[3];
				String source = strs[7];

				String channel = NeteaseChannel_CS.getChannelName(url);

				iotk.getHm().put(channel + "_count", 1);

				if (source.equals("ph")) {
					iotk.getHm().put(channel + "_ph_count", 1);
				} else if (source.equals("wb")) {
					iotk.getHm().put(channel + "_wb_count", 1);
				} else if (source.equals("zq")) {
					iotk.getHm().put(channel + "_zq_count", 1);
				} else if (source.equals("3g")) {
					iotk.getHm().put(channel + "_3g_count", 1);
				} else {
					iotk.getHm().put(channel + "_other_count", 1);
				}
				map.put("uid", uid);
				map.put("channel", channel);
				outputKey.set(JsonUtils.toJson(map));
				context.write(outputKey, iotk);
			} catch (Exception e) {
				context.getCounter("GenTieInfoMapper", "mapException")
						.increment(1);
			}
		}
	}

	public static class DistinctReducer extends
			Reducer<Text, HashMapStringIntegerWritable, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private HashMap<String, Integer> hm = new HashMap<String, Integer>();

		@Override
		protected void reduce(Text key, Iterable<HashMapStringIntegerWritable> values,
				Context context) throws IOException, InterruptedException {

			Map<String, String> map = JsonUtils.json2Map(key.toString());
			String channel = map.get("channel");
			ArrayList<String> al = new ArrayList<String>();

			for (HashMapStringIntegerWritable val : values) {
				for (String s : val.getHm().keySet()) {
					if (hm.get(s) != null) {
						hm.put(s, new Integer(hm.get(s) + val.getHm().get(s)));
					} else {
						hm.put(s, new Integer(val.getHm().get(s)));
					}

					String source = s.split("_")[1];
					if ((!al.contains(source)) && (!source.equals("count"))) {
						al.add(source);
					}

				}

			}

			for (String s : al) {
				if (hm.get(channel + "_" + s + "_uv") != null) {
					hm.put(channel + "_" + s + "_uv",
							hm.get(channel + "_" + s + "_uv") + 1);
				} else {
					hm.put(channel + "_" + s + "_uv", 1);
				}
			}
			if (hm.get(channel + "_uv") != null) {
				hm.put(channel + "_uv", hm.get(channel + "_uv") + 1);
			} else {
				hm.put(channel + "_uv", 1);
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
            ArrayList<String> vaildChannel =new ArrayList<String>();

			String[] vaildzhibiao = new String[] { "count", "ph_count",
					"wb_count", "zq_count", "3g_count", "other_count", "uv",
					"ph_uv", "wb_uv", "zq_uv", "3g_uv", "other_uv" };
			
			for(String key :hm.keySet()){
				String channel = key.split("_")[0];
				if (!vaildChannel.contains(channel)){
					vaildChannel.add(channel);
				}
			}
			for (String channel : vaildChannel) {
				StringBuilder sb = new StringBuilder();

				for (String zhibiao : vaildzhibiao) {
					sb.append(
							hm.get(channel + "_" + zhibiao) == null ? 0 : hm
									.get(channel + "_" + zhibiao)).append("\t");
				}

				outputKey.set(channel);
				outputValue.set(sb.toString().trim());
				context.write(outputKey, outputValue);
			}

		}

	}

}
