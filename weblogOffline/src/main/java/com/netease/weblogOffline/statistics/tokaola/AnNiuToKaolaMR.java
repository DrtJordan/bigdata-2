package com.netease.weblogOffline.statistics.tokaola;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.LoggerFactory;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.weblogfilter.WeblogFilterUtils;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 按钮考拉回流统计
 * 
 */

public class AnNiuToKaolaMR extends MRJob {

	private static final org.slf4j.Logger log = LoggerFactory
			.getLogger(AnNiuToKaolaMR.class);

	@Override
	public boolean init(String date) {
		// weblog输入目录
	//	inputList.add(DirConstant.WEBLOG_LOG + date);
		inputList.add(DirConstant.WEBLOG_FilterLOG
				+ date);
		// navlog输入目录
//		inputList.add("/ntes_weblog/nav/navLog/nav/" + date);
		// 输出列表
		outputList.add(DirConstant.WEBLOG_STATISTICS_DIR+"result_toDC/AnNiuToKaola/" + date);

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

		// mapperoutput
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setCombinerClass(logReducer.class);
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
			Mapper<NullWritable, Text, Text, IntWritable> {
		IntWritable  outValue = new IntWritable(1);
		@Override
		public void map(NullWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			try {

				HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());		

                String cdata_href = lineMap.get("cdata_href");
                String event = lineMap.get("event");
                String url =lineMap.get("url");
  
				if (url.equals("http://money.163.com/special/app_kaola_pc/")&&event.equals("click")
						&& cdata_href.equals("http://www.kaola.com/")) {

					context.write(new Text("http://www.kaola.com/"), outValue);
				}else if(url.equals("http://money.163.com/special/app_kaola_pc/")&&event.equals("click")
						&& cdata_href.equals("https://itunes.apple.com/cn/app/wang-yi-cai-jing/id910846410?mt=8")){
					context.write(new Text("https://itunes.apple.com/cn/app/wang-yi-cai-jing/id910846410?mt=8"), outValue);
				}else if(url.equals("http://money.163.com/special/app_kaola_pc/")&&event.equals("click")
						&& cdata_href.equals("http://file.ws.126.net/finance/imoney/android/netease_imoney_2.1.3_netease_imoney.apk")){
					context.write(new Text("http://file.ws.126.net/finance/imoney/android/netease_imoney_2.1.3_netease_imoney.apk"), outValue);
				}

			} catch (Exception e) {
				context.getCounter("WeblogMapper", "parseError")
						.increment(1);
			}
		}

	}


	public static class logReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {


		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

             int sum =0;
			for (IntWritable val : values) {
		        sum +=  new Integer(val.get());
			}
			context.write(key, new IntWritable(sum));	
		}

	}

}
