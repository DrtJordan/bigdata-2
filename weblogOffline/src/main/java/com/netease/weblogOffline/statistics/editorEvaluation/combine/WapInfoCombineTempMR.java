package com.netease.weblogOffline.statistics.editorEvaluation.combine;
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
import com.netease.weblogCommon.data.enums.ContentAttributions;
import com.netease.weblogCommon.data.enums.Platform;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.HashMapStringStringWritable;
import com.netease.weblogOffline.data.MultiStatisticResultWrapWritable;
import com.netease.weblogOffline.data.MultiStatisticResultWritable;
import com.netease.weblogOffline.utils.HadoopUtils;
public class WapInfoCombineTempMR extends MRJob {

	@Override
	public boolean init(String date) {
 		inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"pvUv_wap/"+ date+"/res/");
 		inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"sessionCount_wap/"+ date+"/res/");
 		inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"baseInfoAnd3gCombineTemp/"+date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"wapInfoCombineTemp/"+date);
		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "_step1");
		
		MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, WapInfoCombineMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, WapInfoCombineMapper.class); 
		MultipleInputs.addInputPath(job, new Path(inputList.get(2)), SequenceFileInputFormat.class, BaseInfoAnd3gCombineMapper.class);

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


	
	public static class WapInfoCombineMapper
	extends
	Mapper<Text, MultiStatisticResultWritable, Text, MultiStatisticResultWrapWritable> {
		private MultiStatisticResultWrapWritable msrww = new MultiStatisticResultWrapWritable();
		@Override
		public void map(Text key, MultiStatisticResultWritable value,
				Context context) throws IOException, InterruptedException {
			msrww.getMsr().getDataMap().clear();
			msrww.getConf().clear();
			msrww.getMsr().add(value);
			context.write(key, msrww);
		}
	}
	
	
	
//	public static class WapInfoCombineGenTieMapper
//	extends
//	Mapper<Text, MultiStatisticResultWritable, Text, MultiStatisticResultWrapWritable> {
//		private  Text outKey  = new Text();
//		private MultiStatisticResultWritable  msrw = new MultiStatisticResultWritable();
//		private MultiStatisticResultWrapWritable msrww = new MultiStatisticResultWrapWritable();
//		private  Text mapKey  = new Text();
//		@Override
//		public void map(Text key, MultiStatisticResultWritable value,
//				Context context) throws IOException, InterruptedException {
//			msrw.getDataMap().clear();
//			msrww.getMsr().getDataMap().clear();
//			msrww.getConf().clear();
//			outKey.set(key.toString().split(",")[1]);
//			
//			mapKey.set(EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.genTie, Platform.wap, ShareBackChannel_EE.all));
////			mapKey.set(EditorEvaluationConstant.StatisticsIndicator.genTie + "_" +EditorEvaluationConstant.Platform.wap +"_"+EditorEvaluationConstant.defValue);
//			if (value.getDataMap().get(mapKey)!=null){
//				msrw.getDataMap().put(mapKey, 
//						value.getDataMap().get(mapKey));
//				msrww.getMsr().add(msrw);
//				context.write(outKey, msrww);
//			}
//		}
//	}
	
	public static class BaseInfoAnd3gCombineMapper extends Mapper<Text, HashMapStringStringWritable, Text, MultiStatisticResultWrapWritable> {
		private MultiStatisticResultWrapWritable outValue = new MultiStatisticResultWrapWritable();
		@Override
		public void map(Text key, HashMapStringStringWritable value, Context context) throws IOException, InterruptedException {
			outValue.getConf().clear();
			outValue.add(value.getHm());
			context.write(key, outValue);
		}
	}
	
	public static class WapInfoCombineReducer extends Reducer<Text, MultiStatisticResultWrapWritable, Text, MultiStatisticResultWrapWritable> {
		private Text outKey = new Text();
		private MultiStatisticResultWrapWritable outValue = new MultiStatisticResultWrapWritable();

		@Override
		protected void reduce(Text key, Iterable<MultiStatisticResultWrapWritable> values, Context context) throws IOException, InterruptedException {
			outValue.getMsr().getDataMap().clear();
			outValue.getConf().clear();
			
			for (MultiStatisticResultWrapWritable val : values) {
				for (Text t : val.getMsr().getDataMap().keySet()) {
					if (t.toString().split("_")[1].equals(Platform.wap.getName())) {
						outValue.getMsr().getDataMap().put(t, val.getMsr().getDataMap().get(t));
					}
				}
				outValue.getConf().putAll(val.getConf());
			}
			
			if (outValue.getConf().get(ContentAttributions.id_3w.getName()) != null) {
				outKey.set(outValue.getConf().get(ContentAttributions.id_3w.getName()) + "," + key);
			} else {
				outKey.set("" + "," + key);
			}
			
			if ((!outKey.toString().equals(",")) && outValue.getMsr().getDataMap().size() > 0) {
				context.write(outKey, outValue);
			}
		}
	}
}
