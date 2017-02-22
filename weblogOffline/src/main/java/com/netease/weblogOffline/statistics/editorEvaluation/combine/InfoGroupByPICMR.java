package com.netease.weblogOffline.statistics.editorEvaluation.combine;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
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
import com.netease.weblogOffline.data.StatisticResultWritable;
import com.netease.weblogOffline.utils.HadoopUtils;
@NotCheckInputFile
public class InfoGroupByPICMR extends MRJob {

	@Override
	public boolean init(String date) {
 		inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"genTieAndPcAppWapInfoCombine/"+date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"infoGroupByPIC/"+date);
		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;
		
		Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "_step1");
		boolean b =false;
		b|=addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, InfoGroupByPICMapper.class);
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


	
	
	public static class InfoGroupByPICMapper extends Mapper<Text, MultiStatisticResultWrapWritable, Text, MultiStatisticResultWrapWritable> {
		private Text outKey = new Text();
		@Override
		public void map(Text key, MultiStatisticResultWrapWritable value, Context context) throws IOException, InterruptedException {
			String editor_3w = value.getConf().get(ContentAttributions.editor_3w.getName());
			String editor_3g = value.getConf().get(ContentAttributions.editor_3g.getName());
			
			if (editor_3w != null && (!"(null)".equals(editor_3w))) {
				outKey.set(editor_3w + ",3w");
				context.write(outKey, value);
			} else if (editor_3g != null && (!"(null)".equals(editor_3g))) {
				outKey.set(editor_3g + ",3g");
				context.write(outKey, value);
			}
		}
	}
		
	public static class InfoCombineReducer extends Reducer<Text, MultiStatisticResultWrapWritable, Text, MultiStatisticResultWrapWritable> {
		private MultiStatisticResultWrapWritable  outValue = new MultiStatisticResultWrapWritable();
		@Override
		protected void reduce(Text key, Iterable<MultiStatisticResultWrapWritable> values, Context context) throws IOException, InterruptedException {
			outValue.getMsr().getDataMap().clear();
			outValue.getConf().clear();
			
			for (MultiStatisticResultWrapWritable val : values) {	
				for(Entry<Text, StatisticResultWritable> e:val.getMsr().getDataMap().entrySet()){
					if (outValue.getMsr().getDataMap().get(e.getKey())==null){
						outValue.getMsr().getDataMap().put(new Text(e.getKey()),new StatisticResultWritable(e.getValue()));
					}else {
						StatisticResultWritable srw = outValue.getMsr().getDataMap().get(e.getKey());
						srw.increasePV(e.getValue().getPv());
						srw.increaseUV(e.getValue().getUv());
					}
				}
				
				String editor_3w = val.getConf().get(ContentAttributions.editor_3w.getName());
				if(StringUtils.isNotBlank(editor_3w)){
					outValue.getConf().put(ContentAttributions.editor_3w.getName(), editor_3w);
				}
				
				String editor_3g = val.getConf().get(ContentAttributions.editor_3g.getName());
				if(StringUtils.isNotBlank(editor_3g)){
					outValue.getConf().put(ContentAttributions.editor_3g.getName(), editor_3g);
				}
			}
			
			if (outValue.getMsr().getDataMap().size() > 0){
				context.write(key, outValue);	
			}		
		}
	}     
}
