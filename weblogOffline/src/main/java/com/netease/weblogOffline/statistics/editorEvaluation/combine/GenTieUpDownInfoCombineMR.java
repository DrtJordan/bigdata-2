package com.netease.weblogOffline.statistics.editorEvaluation.combine;
import java.io.IOException;
import java.util.Map.Entry;

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
import com.netease.weblogOffline.data.GenTieBaseWritable;
import com.netease.weblogOffline.data.MultiStatisticResultWrapWritable;
import com.netease.weblogOffline.data.MultiStatisticResultWritable;
import com.netease.weblogOffline.data.StatisticResultWritable;
import com.netease.weblogOffline.utils.HadoopUtils;
public class GenTieUpDownInfoCombineMR extends MRJob {

	@Override
	public boolean init(String date) {
 		inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"articleUpDown_www/"+ date+"/res/");
 		inputList.add(DirConstant.GENTIE_BASE_ALL + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"genTieUpDownInfoCombine/"+date);
		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;
		
			Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "_step1");
			
			MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, GenTieUpDownInfoCombineMapper.class);
			MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, GenTieBaseInfoCombineMapper.class); 
			
			// mapper
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(MultiStatisticResultWrapWritable.class);

			// reducer
			job.setReducerClass(GenTieUpDownInfoCombineReducer.class);
			job.setNumReduceTasks(16);
			FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(MultiStatisticResultWrapWritable.class);
			
			if (!job.waitForCompletion(true)) {
				jobState = JobConstant.FAILED;
			}
			
			return jobState;
	}


	
	public static class GenTieUpDownInfoCombineMapper extends Mapper<Text, MultiStatisticResultWritable, Text, MultiStatisticResultWrapWritable> {
		private MultiStatisticResultWrapWritable outValue = new MultiStatisticResultWrapWritable();
		@Override
		public void map(Text key, MultiStatisticResultWritable value, Context context) throws IOException, InterruptedException {
			outValue.getMsr().getDataMap().clear();
			outValue.getMsr().add(value);
			context.write(key, outValue);
		}
	}
	
	public static class GenTieBaseInfoCombineMapper extends Mapper<Text, GenTieBaseWritable, Text, MultiStatisticResultWrapWritable> {
		private Text outKey = new Text();
		private MultiStatisticResultWrapWritable outValue = new MultiStatisticResultWrapWritable();
		@Override
		public void map(Text key, GenTieBaseWritable value, Context context) throws IOException, InterruptedException {
			outKey.set(value.getDocid());
			outValue.getConf().clear();
			outValue.getConf().put("url", value.getUrl());
			outValue.getConf().put("pdocid", value.getPdocid());
			outValue.getConf().put("docid", value.getDocid());
			context.write(outKey, outValue);
		}
	}
	
	public static class GenTieUpDownInfoCombineReducer extends Reducer<Text, MultiStatisticResultWrapWritable, Text, MultiStatisticResultWrapWritable> {
		private Text outKey = new Text();
		private MultiStatisticResultWrapWritable outValue = new MultiStatisticResultWrapWritable();

		@Override
		protected void reduce(Text key, Iterable<MultiStatisticResultWrapWritable> values, Context context) throws IOException, InterruptedException {
			outValue.getMsr().getDataMap().clear();
			outValue.getConf().clear();
			
			for (MultiStatisticResultWrapWritable val : values) {
				for (Entry<Text, StatisticResultWritable> e : val.getMsr().getDataMap().entrySet()) {
					outValue.getMsr().getDataMap().put(new Text(e.getKey()), new StatisticResultWritable(e.getValue()));
				}

				for (Entry<String, String> e : val.getConf().entrySet()) {
					outValue.getConf().put(e.getKey(), e.getValue());
				}
			}
			
			if (outValue.getConf().get("url") != null) {
				outKey.set(key + "," + outValue.getConf().get("url"));
			} else {
				outKey.set(key + "," + "");
			}
			
			outValue.getConf().clear();
			
			if ((!outKey.toString().equals(","))) {
				context.write(outKey, outValue);
			}
		}
	}
}
