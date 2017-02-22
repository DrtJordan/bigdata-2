package com.netease.weblogOffline.statistics.editorEvaluation.combine;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.annotation.NotCheckInputFile;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.HashMapStringStringWritable;
import com.netease.weblogOffline.data.LongHashMapStringStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;
@NotCheckInputFile
public class BaseInfoAnd3gIncrMR extends MRJob {

	@Override
	public boolean init(String date) {
		try {
			String theDayBefore = DateUtils.getTheDayBefore(date, 1);
			inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION
					+ "contentInfoAll/" + theDayBefore);
			inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION
					+ "contentInfoAll/" + date);
			outputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION
					+ "baseInfoAnd3gIncr/" + date);
			return true;

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			LOG.error(e);
			return false;
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName()
				+ "_step1");
		boolean b =false;
		b|=addInputPath(job, new Path(inputList.get(0)),
				SequenceFileInputFormat.class,
				BaseInfoAnd3gIncrBeforeMapper.class);
		b|=addInputPath(job, new Path(inputList.get(1)),
				SequenceFileInputFormat.class, BaseInfoAnd3gIncrMapper.class);
		if (!b){
			return jobState;
		}
		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongHashMapStringStringWritable.class);

		// reducer
		job.setReducerClass(BaseInfoAnd3gIncrReducer.class);
		job.setNumReduceTasks(16);
		FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(HashMapStringStringWritable.class);

		if (!job.waitForCompletion(true)) {
			jobState = JobConstant.FAILED;
		}
		return jobState;

	}

	public static class BaseInfoAnd3gIncrMapper
			extends
			Mapper<Text, HashMapStringStringWritable, Text, LongHashMapStringStringWritable> {
		LongHashMapStringStringWritable hmss = new LongHashMapStringStringWritable();

		@Override
		public void map(Text key, HashMapStringStringWritable value,
				Context context) throws IOException, InterruptedException {
			hmss.setSecond(value);
			hmss.setFirst(1);
			context.write(key, hmss);
		}
	}

	public static class BaseInfoAnd3gIncrBeforeMapper
			extends
			Mapper<Text, HashMapStringStringWritable, Text, LongHashMapStringStringWritable> {
		LongHashMapStringStringWritable hmss = new LongHashMapStringStringWritable();

		@Override
		public void map(Text key, HashMapStringStringWritable value,
				Context context) throws IOException, InterruptedException {
			hmss.setSecond(value);
			hmss.setFirst(0);
			context.write(key, hmss);
		}
	}

	public static class BaseInfoAnd3gIncrReducer
			extends
			Reducer<Text, LongHashMapStringStringWritable, Text, HashMapStringStringWritable> {
		HashMapStringStringWritable hmss1 = new HashMapStringStringWritable();
		HashMapStringStringWritable hmss0 = new HashMapStringStringWritable();

		@Override
		protected void reduce(Text key,
				Iterable<LongHashMapStringStringWritable> values, Context context)
				throws IOException, InterruptedException {
			hmss1.getHm().clear();
			hmss0.getHm().clear();

			for (LongHashMapStringStringWritable val : values) {
				if (val.getFirst()==1) {
					hmss1.getHm().putAll(val.getSecond().getHm());
				} else {
					hmss0.getHm().putAll(val.getSecond().getHm());
				}

			}
			if (!hmss1.getHm().equals(hmss0.getHm())){
				context.write(key, hmss1);
			}
			
		}
	}
}
