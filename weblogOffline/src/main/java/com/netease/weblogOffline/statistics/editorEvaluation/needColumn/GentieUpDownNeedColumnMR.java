package com.netease.weblogOffline.statistics.editorEvaluation.needColumn;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.LongStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;


/**
 * 提取字段
 * 
 */

public class GentieUpDownNeedColumnMR extends MRJob {

	@Override
	public boolean init(String date) {
		// 输入列表
		inputList.add(DirConstant.GENTIE_VOTE_LOG + date);

		// 输出列表
		outputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION+"gentieUpDownNeedColumn/" + date);

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
				TextInputFormat.class, GentieUpDownNeedColumnMapper.class);

		
//		MultipleInputs.addInputPath(job, new Path(inputList.get(0)), CombineSmallfileInputFormat.class,
//				WebLogFilterMapper.class);
		
		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// reducer
		job.setReducerClass(GentieUpDownNeedColumnReducer.class);
		job.setNumReduceTasks(64);
		FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);


		if (!job.waitForCompletion(true)) {
			jobState = JobConstant.FAILED;
		}

		return jobState;
	}

	public static class GentieUpDownNeedColumnMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		private Text outKey = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				HashMap<String, String> genTieUpDownLogParserMap = NeedColumnUtils.genTieUpDownParser(value.toString());
                StringBuilder sb = new StringBuilder();
                String dota = "";
			    for (String s :NeedColumnUtils.getNeedColumnsOfGentieUpDown()){
			    	sb.append(dota).append(genTieUpDownLogParserMap.get(s));
			    	dota=",";
			    }
				outKey.set(sb.toString());
				

				context.write(outKey, NullWritable.get());
		
			} catch (Exception e) {
				context.getCounter("ZyFilterMapper", "parseError").increment(1);
			}

		}
	}

	public static class GentieUpDownNeedColumnReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

		


		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			
			
			
			ArrayList<LongStringWritable> al = new ArrayList<LongStringWritable>();
		
			for (NullWritable val : values) {

				context.write(key, NullWritable.get());
			}
			
		}

	}
}
