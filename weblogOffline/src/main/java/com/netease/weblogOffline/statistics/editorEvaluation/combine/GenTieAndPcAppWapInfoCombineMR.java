package com.netease.weblogOffline.statistics.editorEvaluation.combine;
import java.io.IOException;
import java.util.Map.Entry;

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
import com.netease.weblogCommon.data.enums.ContentAttributions;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.MultiStatisticResultWrapWritable;
import com.netease.weblogOffline.data.MultiStatisticResultWritable;
import com.netease.weblogOffline.data.StatisticResultWritable;
import com.netease.weblogOffline.utils.HadoopUtils;
@NotCheckInputFile
public class GenTieAndPcAppWapInfoCombineMR extends MRJob {

	@Override
	public boolean init(String date) {
 		inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"pcAppWapInfoCombine/"+date);
 		inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"genTie/"+ date+"/res/");
 	    inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"genTieUpDownInfoCombine/"+date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"genTieAndPcAppWapInfoCombine/"+date);
		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;
		
			Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "_step1");
			boolean b =false;
			b|=addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, GenTieAndAllInfoCombineMapper.class);
			b|=addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, InfoCombineGenTieMapper.class); 
			b|=addInputPath(job, new Path(inputList.get(2)), SequenceFileInputFormat.class, GenTieAndAllInfoCombineMapper.class); 
			
			if (!b){
				return jobState;
			}
			// mapper
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(MultiStatisticResultWrapWritable.class);

			// reducer
			job.setReducerClass(InfoCombineReducer.class);
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


	
	
	public static class GenTieAndAllInfoCombineMapper extends Mapper<Text, MultiStatisticResultWrapWritable, Text, MultiStatisticResultWrapWritable> {
		private Text outKey = new Text();
		@Override
		public void map(Text key, MultiStatisticResultWrapWritable value, Context context) throws IOException, InterruptedException {
			try {
				String s[] = key.toString().split(",");
				if (s.length == 2) {
					outKey.set(s[1]);
				} else if (s.length > 2) {
					String url = s[1];
					for (int i = 2; i < s.length; i++) {
						url = url + "," +  s[i];
					}
					outKey.set(url);
				} else {
					outKey.set(key);
				}
				context.write(outKey, value);
			} catch (Exception e) {
				context.getCounter("WapInfoCombineMapper", "exception").increment(1);
			}
		}
	}
	
	public static class InfoCombineGenTieMapper extends Mapper<Text, MultiStatisticResultWritable, Text, MultiStatisticResultWrapWritable> {
		private  Text outKey  = new Text();
		private MultiStatisticResultWrapWritable outValue = new MultiStatisticResultWrapWritable();
		@Override
		public void map(Text key, MultiStatisticResultWritable value, Context context) throws IOException, InterruptedException {
			try{		
				String s[] = key.toString().split(",");
				if (s.length==2){
					outKey.set(s[1]);
				}else if (s.length>2){
					String url = s[1];
					for (int i = 2; i < s.length; i++) {
						url = url + "," +  s[i];
					}
					outKey.set(url);
				}else {
					outKey.set(key);
				}
				
				outValue.getMsr().getDataMap().clear();
				outValue.getMsr().add(value);
				context.write(outKey, outValue);
			}catch (Exception e){
		    	context.getCounter("WapInfoCombineMapper", key.toString()).increment(1);
		    }
		}
}
	
	
	
	public static class InfoCombineReducer extends Reducer<Text, MultiStatisticResultWrapWritable, Text, MultiStatisticResultWrapWritable> {
		private  Text outKey  = new Text();
		private MultiStatisticResultWrapWritable outValue = new MultiStatisticResultWrapWritable();
		@Override
		protected void reduce(Text key, Iterable<MultiStatisticResultWrapWritable> values, Context context) throws IOException, InterruptedException {
			outValue.getMsr().getDataMap().clear();
			outValue.getConf().clear();
			
			for (MultiStatisticResultWrapWritable val : values) {	
				for(Entry<Text, StatisticResultWritable> e:val.getMsr().getDataMap().entrySet()){
					outValue.getMsr().getDataMap().put(new Text(e.getKey()),new StatisticResultWritable(e.getValue()));
				}
				
				for(Entry<String, String> e:val.getConf().entrySet()){
					outValue.getConf().put(e.getKey(),e.getValue());
				}
			}
			
			if (outValue.getMsr().getDataMap().size() > 0){
				String id = outValue.getConf().get(ContentAttributions.id_3w.getName());
				if (id == null) {
					id="";
				} 
				
				if (key.toString().endsWith(",")){
					outKey.set(key);
				}else {
					outKey.set(id + "," + key);
				}
				
				context.write(outKey, outValue);		
			}		
		}
	}     
}
