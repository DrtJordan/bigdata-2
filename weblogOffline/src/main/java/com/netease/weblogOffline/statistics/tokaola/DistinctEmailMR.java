package com.netease.weblogOffline.statistics.tokaola;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.LoggerFactory;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.weblogfilter.WeblogFilterUtils;
import com.netease.weblogOffline.utils.HadoopUtils;


/**
 *考拉回流DistinctEmail统计
 * 
 */

public class DistinctEmailMR extends MRJob {

	private static final org.slf4j.Logger log = LoggerFactory
			.getLogger(DistinctEmailMR.class);

	@Override
	public boolean init(String date) {
		// weblog输入目录
		//inputList.add(DirConstant.WEBLOG_LOG + date);
		
		inputList.add(DirConstant.WEBLOG_FilterLOG
		+ date);
		// navlog输入目录
		inputList.add("/ntes_weblog/nav/navLog/nav/" + date);
		// 输出列表
		outputList.add("/ntes_weblog/weblog/temp/Kaola/DistinctEmail/" + date);

		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job = HadoopUtils
				.getJob(this.getClass(), this.getClass().getName());

		Configuration conf = job.getConfiguration();

		// weblog
		MultipleInputs.addInputPath(job, new Path(inputList.get(0)),
				SequenceFileInputFormat.class, WeblogMapper.class);

		// navlog
		MultipleInputs.addInputPath(job, new Path(inputList.get(1)),
				TextInputFormat.class, NavlogMapper.class);

		// mapperoutput
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// reducer
		job.setReducerClass(DistinctEmailReducer.class);
		job.setNumReduceTasks(8);
		FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		if (!job.waitForCompletion(true)) {
			jobState = JobConstant.FAILED;
		}

		return jobState;
	}

	public static class WeblogMapper extends
			Mapper<NullWritable, Text, Text, NullWritable> {

	//	private static final LogParser logParser = new WeblogParser();

		@Override
		public void map(NullWritable key, Text value, Context context)
				throws IOException, InterruptedException {

//			Map<String, String> logMap = new HashMap<String, String>();

			try {
				HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());
                String  email = lineMap.get("cvar_email");
                String cdata_href =lineMap.get("cdata_href");
                String event = lineMap.get("event");
		//		logMap = logParser.parse(value.toString());
		//		String email = stringProcess(logMap.get("cvar_email"));


		//		String cdata_href = stringProcess(logMap.get("cdata_href"));
		//		String event = stringProcess(logMap.get("event"));

				if (event.equals("click")
						&& cdata_href.indexOf("www.kaola.com") != -1) {

					
					if (!email.equals("(null)")) {
						context.write(new Text(email), NullWritable.get());
					}

					
				}

			} catch (Exception e) {
				context.getCounter("WeblogMapper", "parseError")
						.increment(1);
			}
		}

//		private String stringProcess(String s) {
//			return (StringUtils.isBlank(s) ? "(null)" : s
//					.trim());
//		}

	}

	public static class NavlogMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		private static final LogParser logParser = new NavlogParser();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			Map<String, String> logMap = new HashMap<String, String>();

			try {
				logMap = logParser.parse(value.toString());
				String email = stringProcess(logMap.get("uemail"));

				String cdata_href = stringProcess(logMap.get("cdata_href"));
				String event = stringProcess(logMap.get("event"));

				if (event.equals("click")
						&& cdata_href.indexOf("www.kaola.com") != -1) {

					
					if (!email.equals("(null)")) {
						context.write(new Text(email), NullWritable.get());
					}

					
				}

			} catch (Exception e) {
				context.getCounter("NavlogMapper", "parseError")
						.increment(1);
			}
		}

		private String stringProcess(String s) {
			return (StringUtils.isBlank(s) ? "(null)" : s
					.trim());
		}

	}

	public static class DistinctEmailReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {


		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {

			context.write(key, NullWritable.get());
		}
	}

}
