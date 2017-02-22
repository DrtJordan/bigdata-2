package com.netease.weblogOffline.common.zylogfilter;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import com.netease.weblogCommon.utils.TextUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.utils.HadoopUtils;


/**
 * 过滤章鱼
 * 
 */

public class ZyLogFilter extends MRJob {

	@Override
	public boolean init(String date) {
		// 输入列表
		inputList.add(DirConstant.ZY_LOG + date);//zy日志

		// 输出列表
		outputList.add(DirConstant.ZYLOG_FilterLOG + date);

		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job = HadoopUtils.getJob(this.getClass(), this.getClass()
				.getName());
		
		
//		job.getConfiguration().setInt("mapred.min.split.size", 1);
//	    job.getConfiguration().setLong("mapred.max.split.size", 128 * 1024 * 1024);
	    
	    

		MultipleInputs.addInputPath(job, new Path(inputList.get(0)),
				TextInputFormat.class, ZyFilterMapper.class);

		
//		MultipleInputs.addInputPath(job, new Path(inputList.get(0)), CombineSmallfileInputFormat.class,
//				WebLogFilterMapper.class);
		
		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// reducer
		job.setReducerClass(ZyFilterReducer.class);
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

	public static class ZyFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		private Text outKey = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				HashMap<String, String> zylogParserMap = ZylogFilterUtils.zylogParser(value.toString());
				StringBuilder sb = new StringBuilder();
				for(String colums : ZylogFilterUtils.getColumns()){
					sb.append(TextUtils.notNullStr(zylogParserMap.get(colums), ZylogFilterUtils.defNullStr)).append("\t");
				}
		
				outKey.set(sb.toString().replace("\n", "").replace("\r", "").trim());
				

				context.write(outKey, NullWritable.get());
		
			} catch (Exception e) {
				context.getCounter("ZyFilterMapper", "parseError").increment(1);
			}

		}
	}

	public static class ZyFilterReducer extends Reducer<Text, NullWritable, NullWritable, Text> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			for (NullWritable val : values) {
				context.write(val, key);
			}
		}

	}
}
