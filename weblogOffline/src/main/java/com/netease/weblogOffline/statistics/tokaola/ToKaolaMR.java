package com.netease.weblogOffline.statistics.tokaola;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.weblogfilter.WeblogFilterUtils;
//import com.netease.weblogOffline.common.logparsers.WeblogParser;
import com.netease.weblogOffline.data.InfoOfToKaola;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 考拉回流统计
 * 
 */

public class ToKaolaMR extends MRJob {

	private static final org.slf4j.Logger log = LoggerFactory
			.getLogger(ToKaolaMR.class);

	@Override
	public boolean init(String date) {
		// weblog输入目录
	//	inputList.add(DirConstant.WEBLOG_LOG + date);
		inputList.add(DirConstant.WEBLOG_FilterLOG
				+ date);
		// navlog输入目录
		inputList.add("/ntes_weblog/nav/navLog/nav/" + date);
		// 输出列表
		outputList.add(DirConstant.WEBLOG_STATISTICS_DIR+"result_toDC/kaola/" + date);

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
				SequenceFileInputFormat.class, LogMapper.class);

		// navlog
		MultipleInputs.addInputPath(job, new Path(inputList.get(1)),
				TextInputFormat.class, NavlogMapper.class);

		// mapperoutput
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoOfToKaola.class);

		// reducer
		job.setReducerClass(logReducer.class);
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		if (!job.waitForCompletion(true)) {
			jobState = JobConstant.FAILED;
		}

		return jobState;
	}

	public static class LogMapper extends
			Mapper<NullWritable, Text, Text, InfoOfToKaola> {

		@Override
		public void map(NullWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			try {

				HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());		
				InfoOfToKaola iotk = new InfoOfToKaola();
                String  email = lineMap.get("cvar_email");
                String cdata_href = lineMap.get("cdata_href");
                String event = lineMap.get("event");
                String url =lineMap.get("url");
                String channel = NeteaseChannel_CS.getChannelName(url);

                String uuid = lineMap.get("uuid");
				if (event.equals("click")
						&& cdata_href.indexOf("www.kaola.com") != -1) {

					if (UrlUtils.is163Home(url)||UrlUtils.isNeteaseHome(url)) {
						iotk.getHm().put("countHome", 1);
					}
					iotk.getHm().put("count", 1);
					if (!email.equals("(null)")) {
						iotk.getHm().put("countEmail", 1);
					}
					if (cdata_href
							.startsWith("http://rd.da.netease.com/redirect")) {
						iotk.getHm().put("countRD", 1);
					}

					iotk.getHm().put(channel.toLowerCase(), 1);

					context.write(new Text(uuid), iotk);
				}

			} catch (Exception e) {
				context.getCounter("WeblogMapper", "parseError")
						.increment(1);
			}
		}

	}

	public static class NavlogMapper extends
			Mapper<LongWritable, Text, Text, InfoOfToKaola> {

		private static final LogParser logParser = new NavlogParser();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			Map<String, String> logMap = new HashMap<String, String>();

			try {

				logMap = logParser.parse(value.toString());
				InfoOfToKaola iotk = new InfoOfToKaola();

				String url = stringProcess(logMap.get("url"));
				String uuid = stringProcess(logMap.get("uuid"));
				String email = stringProcess(logMap.get("uemail"));

				String cdata_href = stringProcess(logMap.get("cdata_href"));
				String event = stringProcess(logMap.get("event"));

				if (event.equals("click")
						&& cdata_href.indexOf("www.kaola.com") != -1) {

					if (UrlUtils.is163Home(url)||UrlUtils.isNeteaseHome(url)) {
						iotk.getHm().put("countHome", 1);
					}
					iotk.getHm().put("count", 1);
					if (!email.equals("(null)")) {
						iotk.getHm().put("countEmail", 1);
					}
					if (cdata_href
							.startsWith("http://rd.da.netease.com/redirect")) {
						iotk.getHm().put("countRD", 1);
					}

					iotk.getHm().put("nav", 1);

					context.write(new Text(uuid), iotk);
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

	public static class logReducer extends
			Reducer<Text, InfoOfToKaola, Text, IntWritable> {

		private Text outKey = new Text();
		private InfoOfToKaola iotk = new InfoOfToKaola();
		private int countuv = 0;

		@Override
		protected void reduce(Text key, Iterable<InfoOfToKaola> values,
				Context context) throws IOException, InterruptedException {


			for (InfoOfToKaola val : values) {
				for (String s : val.getHm().keySet()) {
					if (iotk.getHm().get(s) != null) {
						iotk.getHm().put(s,
								iotk.getHm().get(s) + val.getHm().get(s));
					} else {
						iotk.getHm().put(s, val.getHm().get(s));
					}
				}

			}
			countuv++;
			iotk.getHm().put("countUV", countuv);
			

		
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			
			int other = iotk.getHm().get("other")==null?0:iotk.getHm().get("other");
			int countHome = iotk.getHm().get("countHome")==null?0:iotk.getHm().get("countHome");
			iotk.getHm().put("other", other-countHome);
			for (String str :iotk.getHm().keySet()){
				
				context.write(new Text(str), new IntWritable(iotk.getHm().get(str)));
			}
//			StringBuilder sb = new StringBuilder();
//
//			sb.append(iotk.getHm().get("count")).append(",")
//					.append(iotk.getHm().get("countUV")).append(",")
//					.append(iotk.getHm().get("countEmail")).append(",")
//					.append(iotk.getHm().get("countRD")).append(",")
//					.append(iotk.getHm().get("countHome")).append(",")
//					.append(iotk.getHm().get("auto")).append(",")
//					.append(iotk.getHm().get("baby")).append(",")
//					.append(iotk.getHm().get("digi")).append(",")
//					.append(iotk.getHm().get("edu")).append(",")
//					.append(iotk.getHm().get("ent")).append(",")
//					.append(iotk.getHm().get("jiankang")).append(",")
//					.append(iotk.getHm().get("lady")).append(",")
//					.append(iotk.getHm().get("men")).append(",")
//					.append(iotk.getHm().get("mobile")).append(",")
//					.append(iotk.getHm().get("money")).append(",")
//					.append(iotk.getHm().get("news")).append(",")
//					.append(iotk.getHm().get("other")).append(",")
//					.append(iotk.getHm().get("sport")).append(",")
//					.append(iotk.getHm().get("tech")).append(",")
//					.append(iotk.getHm().get("travel")).append(",")
//					.append(iotk.getHm().get("nav"));
//
//			outKey.set(sb.toString());
//			context.write(new Text(), NullWritable.get());
		}
	}

}
