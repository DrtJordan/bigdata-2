package com.netease.weblogOffline.statistics.editorEvaluation;

import com.hadoop.mapreduce.LzoTextInputFormat;
import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.utils.DirUtils;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.utils.HadoopUtils;
import com.netease.weblogOffline.utils.StringUtils.OriginalLogKeys;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MobileHiveLogParseMR extends MRJob {

	@Override
	public boolean init(String date) {
//		for(String p : DirUtils.getMobileHourPath(DirConstant.MOBILE_LOG, date)){
//			inputList.add(p);
//		}
		inputList.add(DirUtils.getMobilePartitionPath(DirConstant.MOBILE_HIVELOG, date));
		outputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION + "mobileHiveLog/" + date);
		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");

		job.getConfiguration().set("dt", getParams().getDate());

		for (String input : inputList) {
			Path path = new Path(input);
			if (getHDFS().exists(path)) {
				MultipleInputs.addInputPath(job, path, SequenceFileInputFormat.class, MobileRawLogMapper.class);
			}
		}
		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// reducer

		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
		job.setOutputFormatClass(TextOutputFormat.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		if (!job.waitForCompletion(true)) {
			jobState = JobConstant.FAILED;
		}
		return jobState;
	}

	public static class MobileRawLogMapper extends Mapper<BytesWritable, Text, Text, NullWritable> {
		private Text outputKey = new Text();
		private NullWritable outputValue = NullWritable.get();

		String[] eventnames = {"_pvX","_ivX","_vvX","_svX","SHARE_NEWS"};
		String[] appids = {"2x1kfBk63z","Gj9YiT","2S5Wcx","S4zbL7","IC8kYG","h4H6BN","J0ylV8","8Bn4W5","2lR24f","tI2rfh","23RfG1","JBfjmI","0V0Gvx","vNjbl9"};


		@Override
		public void map(BytesWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			Map<String, String> valueMap = new HashMap<String, String>();
			try {
				String infos[] = line.split("\001");
				if(infos.length!=22){
					context.getCounter("MobileRawLogMapper", "not24").increment(1);
				}else {
					if(java.util.Arrays.asList(appids).contains(infos[3])&&
							java.util.Arrays.asList(eventnames).contains(infos[4])) {
						valueMap.put("sessionid", infos[1]);
						valueMap.put("app_id", infos[3]);
						valueMap.put("eventname", infos[4]);
						valueMap.put("eventtag", infos[5]);
						valueMap.put("uuid", infos[6]);
						valueMap.put("tstime", infos[13]);
						valueMap.put("acc", infos[15]);
						valueMap.put("attr1", infos[16]);
						valueMap.put("attr2", infos[17]);
						outputKey.set(JsonUtils.toJson(valueMap));
						context.write(outputKey, outputValue);
					}
				}
			} catch (Exception e) {
				context.getCounter("MobileRawLogMapper", "mapException").increment(1);
			}
		}
	}
}

