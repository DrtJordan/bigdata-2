package com.netease.weblogOffline.statistics.editorEvaluation.combine;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
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
import com.netease.weblogCommon.data.enums.ContentAttributions;
import com.netease.weblogCommon.data.enums.Platform;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.MultiStatisticResultWrapWritable;
import com.netease.weblogOffline.data.MultiStatisticResultWritable;
import com.netease.weblogOffline.data.StatisticResultWritable;
import com.netease.weblogOffline.utils.HadoopUtils;
public class WapInfoAndShareCombineMR extends MRJob {

	@Override
	public boolean init(String date) {
 		inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"wapInfoCombineTemp/"+date);
		inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"spsShareBack/"+ date+"/res/");
		outputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"wapInfoAndShareCombine/"+date);
		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "_step1");
		
		MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, WapInfoCombineMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, WapInfoCombineShareOrBackMapper.class); 
		
		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MultiStatisticResultWrapWritable.class);

		// reducer
		job.setReducerClass(WapInfoCombineReducer.class);
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


	

	public static class WapInfoCombineMapper extends Mapper<Text, MultiStatisticResultWrapWritable, Text, MultiStatisticResultWrapWritable> {
		private Text outKey = new Text();
		@Override
		public void map(Text key, MultiStatisticResultWrapWritable value, Context context) throws IOException, InterruptedException {
	       try{
	    		String id = key.toString().split(",")[0];
				if (StringUtils.isNotBlank(id)){
					outKey.set(id);
					context.write(outKey, value);
				}else {
					context.write(key, value);
				}
	       }catch (Exception e){
	    	   context.getCounter("WapInfoCombineMapper", "exception").increment(1);
	       }
		}
	}
	
	public static class WapInfoCombineShareOrBackMapper extends Mapper<Text, MultiStatisticResultWritable, Text, MultiStatisticResultWrapWritable> {
		private MultiStatisticResultWrapWritable msrww = new MultiStatisticResultWrapWritable();
		@Override
		public void map(Text key, MultiStatisticResultWritable value, Context context) throws IOException, InterruptedException {
			msrww.getMsr().getDataMap().clear();
			msrww.getMsr().add(value);
			context.write(key, msrww);
		}
	}
	
	
	
	public static class WapInfoCombineReducer extends Reducer<Text, MultiStatisticResultWrapWritable, Text, MultiStatisticResultWrapWritable> {
		private Text outKey = new Text();
		private MultiStatisticResultWrapWritable  outValue = new MultiStatisticResultWrapWritable();
		
		@Override
		protected void reduce(Text key, Iterable<MultiStatisticResultWrapWritable> values, Context context) throws IOException, InterruptedException {
			outValue.getMsr().getDataMap().clear();
			outValue.getConf().clear();
			
			for (MultiStatisticResultWrapWritable val : values) {				
				for (Text t:val.getMsr().getDataMap().keySet()){
					if (t.toString().split("_")[1].equals(Platform.wap.getName())){
						outValue.getMsr().getDataMap().put(new Text(t), new StatisticResultWritable(val.getMsr().getDataMap().get(t)));
					}
				}
				
				outValue.getConf().putAll(val.getConf());
			}	
			
			String id = key.toString().split(",")[0];
			
			if (outValue.getConf().get(ContentAttributions.url_3w.getName())!=null){
				
				if (!id.equals("")){
					outKey.set(id+","+outValue.getConf().get(ContentAttributions.url_3w.getName()));
				}else {
					outKey.set(key);
				}
			}else {
		
				if (!id.equals("")){
					outKey.set(id+","+"");
				}else {
					outKey.set(key);
				}
			}		
			
			if (outValue.getMsr().getDataMap().size()>0){
				context.write(outKey, outValue);
			}
		 }
	}    
}

