package com.netease.weblogOffline.common.weblogfilter;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.logparsers.WeblogParser;
import com.netease.weblogCommon.utils.TextUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.utils.HadoopUtils;


/**
 * 过滤大数据
 * 
 */

public class WebLogFilter extends MRJob {

	@Override
	public boolean init(String date) {
		// 输入列表
		inputList.add(DirConstant.WEBLOG_LOG + date);// weblog
		// inputList.add(DirConstant.WEBLOG_LOG +"test");//weblog
		// 输出列表
	  // 	outputList.add("/ntes_weblog/weblog/statistics/temp/WebLogFilter2/"+ date);
		outputList.add(DirConstant.WEBLOG_FilterLOG + date);

		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job = HadoopUtils.getJob(this.getClass(), this.getClass()
				.getName());
		
		job.getConfiguration().setInt("mapreduce.input.fileinputformat.split.maxsize", 512 * 1024 * 1024);	
//		job.getConfiguration().setInt("mapred.min.split.size", 1);
//	    job.getConfiguration().setLong("mapred.max.split.size", 128 * 1024 * 1024);
	    
	    
		String date = this.getParams().getDate();
		job.getConfiguration().set("date", date);

//		MultipleInputs.addInputPath(job, new Path(inputList.get(0)),
//				TextInputFormat.class, WebLogFilterMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputList.get(0)), CombineTextInputFormat.class,
				WebLogFilterMapper.class);
		
//		MultipleInputs.addInputPath(job, new Path(inputList.get(0)), CombineSmallfileInputFormat.class,
//				WebLogFilterMapper.class);
		
		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// reducer
		job.setReducerClass(WebLogFilterReducer.class);
		job.setNumReduceTasks(64);
		FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);


		if (!job.waitForCompletion(true)) {
			jobState = JobConstant.FAILED;
		}

		return jobState;
	}

	public static class WebLogFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		private static final WeblogParser webLogParser = new WeblogParser();
		private Text outKey = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				HashMap<String, String> weblogParserWrapperMap = WeblogFilterUtils.weblogParserWrapper(webLogParser, value.toString());
				StringBuilder sb = new StringBuilder();
				for(String colums : WeblogFilterUtils.getColumns()){
					sb.append(TextUtils.notNullStr(weblogParserWrapperMap.get(colums), WeblogFilterUtils.defNullStr)).append("\t");
				}
		
				outKey.set(sb.toString().replace("\n", "").replace("\r", "").trim());
				

				context.write(outKey, NullWritable.get());
		
			} catch (Exception e) {
				context.getCounter("WebLogFilterMapper", "parseError").increment(1);
			}

		}
	}

	public static class WebLogFilterReducer extends Reducer<Text, NullWritable, NullWritable, Text> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			for (NullWritable val : values) {
				context.write(val, key);
			}
		}

	}
}
