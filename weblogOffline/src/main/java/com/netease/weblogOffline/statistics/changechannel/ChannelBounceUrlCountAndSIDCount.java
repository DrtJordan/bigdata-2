package com.netease.weblogOffline.statistics.changechannel;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import com.netease.weblogOffline.data.ThreeStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;


/**
 * Channel BounceUrl总次数 
 * BounceUrl总sid数
 */

public class ChannelBounceUrlCountAndSIDCount extends MRJob {

	private static final org.slf4j.Logger log = LoggerFactory
			.getLogger(ChannelBounceUrlCountAndSIDCount.class);

	@Override
	public boolean init(String date) {
		// 输入列表

		inputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR
				+ "ChannelBroadurlSIDCount/" + date);
		inputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR +"ChannelBounceUrl/" + date);
		// 输出列表
		outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR
				+ "ChannelBroadBounceUrlInfo/" + date);

		outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR
				+ "ChannelBounceUrlCountAndSIDCount/" + date);

		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass()
				.getName() + "-step1");

		MultipleInputs.addInputPath(job1, new Path(inputList.get(0)),
				SequenceFileInputFormat.class, ChannelBounceUrlSIDCountMapper.class);
		MultipleInputs.addInputPath(job1, new Path(inputList.get(1)),
				SequenceFileInputFormat.class, ChannelBounceUrlInfoMapper.class);
	

		// mapper
		job1.setMapOutputKeyClass(ThreeStringWritable.class);
		job1.setMapOutputValueClass(IntWritable.class);

		// reducer
		job1.setReducerClass(ChannelBounceUrlInfoReducer.class);
		job1.setNumReduceTasks(16);
		FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		job1.setOutputKeyClass(ThreeStringWritable.class);
		job1.setOutputValueClass(IntWritable.class);

		if (!job1.waitForCompletion(true)) {
			jobState = JobConstant.FAILED;
		}

		Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass()
				.getName() + "-step2");

		MultipleInputs.addInputPath(job2, new Path(outputList.get(0)),
				SequenceFileInputFormat.class,
				ChannelBounceUrlCountPvMapper.class);

		// mapper
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);

		// reducer
		job2.setReducerClass(ChannelBounceUrlCountPvReducer.class);
		job2.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		if (!job2.waitForCompletion(true)) {
			jobState = JobConstant.FAILED;
		}

		return jobState;
	}

	public static class ChannelBounceUrlSIDCountMapper
			extends
			Mapper<ThreeStringWritable, IntWritable, ThreeStringWritable, IntWritable> {

		@Override
		public void map(ThreeStringWritable key, IntWritable value,
				Context context) throws IOException, InterruptedException {

			context.write(key, value);

		}

	}
	public static class ChannelBounceUrlInfoMapper
			extends
			Mapper<ThreeStringWritable, IntWritable, ThreeStringWritable, IntWritable> {
		
		  @Override
		   public void map(ThreeStringWritable key, IntWritable value,
				Context context) throws IOException, InterruptedException {
		
			context.write(key, value);
			if (key.getsecond().indexOf("-")!=-1){
				context.write(new ThreeStringWritable(key.getfirst(),key.getsecond().split("-")[0],key.getthird()), value);	
			}

		
		}

}

	public static class ChannelBounceUrlInfoReducer extends
			Reducer<ThreeStringWritable, IntWritable, ThreeStringWritable, IntWritable> {


		@Override
		protected void reduce(ThreeStringWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
	
			 int count= 0;
	            int pv = 0;
				for (IntWritable val : values) {
				
			      if (val.get()==-1){
			    		count--; 
			      }else {
			    	  pv = val.get();
			      }
				}
				if (pv>0&&count<0){
					context.write(key, new IntWritable(count));
					context.write(key, new IntWritable(pv));
				}

		}
	}

	public static class ChannelBounceUrlCountPvMapper extends
			Mapper<ThreeStringWritable, IntWritable, Text, IntWritable> {
		private Text outputKey = new Text();

		@Override
		public void map(ThreeStringWritable key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			outputKey.set(key.getfirst()+"_"+key.getsecond());
			context.write(outputKey, value);
			
		}
	}

	public static class ChannelBounceUrlCountPvReducer extends
			Reducer<Text, IntWritable, Text, Text> {
        Text outValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int count = 0;
			int pv = 0;

			for (IntWritable val : values) {
				if (val.get()<0){
					count+=Math.abs(val.get());
				}else {
					pv += val.get();
				}
			}
			StringBuilder sb = new StringBuilder();
			sb.append(count).append("\t").append(pv);
			outValue.set(sb.toString());
			context.write(key, outValue);
		}

	
	}

}
