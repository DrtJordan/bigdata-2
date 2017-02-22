package com.netease.weblogOffline.statistics.editorEvaluation.combine;
import java.io.IOException;

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
import com.netease.weblogCommon.data.enums.Platform;
import com.netease.weblogCommon.utils.TextUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.HashMapStringStringWritable;
import com.netease.weblogOffline.data.MultiStatisticResultWrapWritable;
import com.netease.weblogOffline.data.MultiStatisticResultWritable;
import com.netease.weblogOffline.data.StatisticResultWritable;
import com.netease.weblogOffline.utils.HadoopUtils;
@NotCheckInputFile
public class AppInfoCombineMR extends MRJob {

	@Override
	public boolean init(String date) {

 		inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"pvUvAndShare_app/"+ date+"/res/");
 		inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"sessionCount_app/"+ date+"/res/");
 		inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"spsShareBack/"+ date+"/res/");
 		inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"baseInfoAnd3gCombineTemp/"+date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"appInfoCombine/"+date);
		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "_step1");
		boolean b = false;
		b |= addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, AppInfoCombineMapper.class);
		b |= addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, AppInfoCombineMapper.class);
		b |= addInputPath(job, new Path(inputList.get(2)), SequenceFileInputFormat.class, AppInfoCombineMapper.class);
		b |= addInputPath(job, new Path(inputList.get(3)), SequenceFileInputFormat.class, BaseInfoAnd3gCombineMapper.class);
		
		if (!b) {
			return jobState;
		}

		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MultiStatisticResultWrapWritable.class);

		// reducer
		job.setReducerClass(AppInfoCombineReducer.class);
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

	
	public static class AppInfoCombineMapper extends Mapper<Text, MultiStatisticResultWritable, Text, MultiStatisticResultWrapWritable> {
		private MultiStatisticResultWrapWritable msrww = new MultiStatisticResultWrapWritable();
		@Override
		public void map(Text key, MultiStatisticResultWritable value, Context context) throws IOException, InterruptedException {
			msrww.getMsr().getDataMap().clear();
			msrww.getMsr().add(value);
			context.write(key, msrww);
		}
	}
	
	public static class BaseInfoAnd3gCombineMapper extends Mapper<Text, HashMapStringStringWritable, Text, MultiStatisticResultWrapWritable> {
		private MultiStatisticResultWrapWritable msrww = new MultiStatisticResultWrapWritable();
		private Text outKey = new Text();
		@Override
		public void map(Text key, HashMapStringStringWritable value, Context context) throws IOException, InterruptedException {
			msrww.getConf().clear();
			msrww.add(value.getHm());
			String id = value.getHm().get(ContentAttributions.id_3g.getName());
			if (id!=null){
				outKey.set(id);
				context.write(outKey, msrww);
			}
		}
	}
	
	public static class AppInfoCombineReducer extends Reducer<Text, MultiStatisticResultWrapWritable, Text, MultiStatisticResultWrapWritable> {
		private MultiStatisticResultWrapWritable msrww = new MultiStatisticResultWrapWritable();
		private Text outKey = new Text();

		@Override
		protected void reduce(Text key, Iterable<MultiStatisticResultWrapWritable> values, Context context) throws IOException, InterruptedException {
			msrww.getMsr().getDataMap().clear();
			msrww.getConf().clear();
			
			for (MultiStatisticResultWrapWritable val : values) {
				for (Text t : val.getMsr().getDataMap().keySet()) {
					if (t.toString().split("_")[1].equals(Platform.app.getName())) {
						msrww.getMsr().getDataMap().put(new Text(t), new StatisticResultWritable(val.getMsr().getDataMap().get(t)));
					}
				}

				msrww.getConf().putAll(val.getConf());
			}
			
			outKey.set(key + "," + TextUtils.notNullStr(msrww.getConf().get(ContentAttributions.url_3w.getName())));

			if ((!outKey.toString().equals(",")) && msrww.getMsr().getDataMap().size() > 0) {
				context.write(outKey, msrww);
			}
		}
	}
}
