package com.netease.weblogOffline.statistics.bigdatahouse;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.HashMapStringStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;
public class MediaSourceMR extends MRJob {
	

	@Override
	public boolean init(String date) {
		//
		inputList.add(DirConstant.HOUSE_URL_MEDIA_ALL + date);
		outputList.add(DirConstant.HOUSE_DAILY_URL_MEDIA + date);
		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

			Job job6 = HadoopUtils.getJob(this.getClass(), this.getClass()
					.getName() + "_step1");

			// Media数据
			MultipleInputs.addInputPath(job6, new Path(inputList.get(0)),
					SequenceFileInputFormat.class, MediaSourceMapper.class);

			// mapper
			job6.setMapOutputKeyClass(Text.class);
			job6.setMapOutputValueClass(HashMapStringStringWritable.class);

			// reducer
			job6.setReducerClass(MediaSourceReducer.class);
			job6.setNumReduceTasks(16);
			FileOutputFormat.setOutputPath(job6, new Path(outputList.get(0)));
			job6.setOutputFormatClass(SequenceFileOutputFormat.class);

			job6.setOutputKeyClass(Text.class);
			job6.setOutputValueClass(HashMapStringStringWritable.class);

			if (!job6.waitForCompletion(true)) {
				jobState = JobConstant.FAILED;
			}
			return jobState;	
		
	}




	public static class MediaSourceMapper
			extends
			Mapper<Text, HashMapStringStringWritable, Text, HashMapStringStringWritable> {

		@Override
		public void map(Text key, HashMapStringStringWritable value,
				Context context) throws IOException, InterruptedException {
			HashMapStringStringWritable hmss = new HashMapStringStringWritable();
			hmss.getHm().put("pure_mediasource", value.getHm().get("mediasource"));
			context.write(key, hmss);
		}
	}

	public static class MediaSourceReducer
			extends
			Reducer<Text, HashMapStringStringWritable, Text, HashMapStringStringWritable> {

		@Override
		protected void reduce(Text key,
				Iterable<HashMapStringStringWritable> values, Context context)
				throws IOException, InterruptedException {

			for (HashMapStringStringWritable val : values) {
				context.write(key, val);
			}
		}
	}
  
      
}
