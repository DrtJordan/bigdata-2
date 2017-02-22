package com.netease.weblogOffline.statistics.contentscore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 *  通过跟帖的数据，计算当日每个频道跟帖uv、每个频道下专题跟帖uv
 * */
public class GenTieChannelSpecialUvMR extends MRJob {
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.GEN_TIE_INFO + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "genTieChannelSpecialUvTemp/" + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "genTieChannelSpecialUv/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");

    	MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), TextInputFormat.class, GenTieInfoMapper.class);
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(NullWritable.class);
        
        //reducer
        job1.setReducerClass(DistinctReducer.class);
        job1.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
      	Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step2");

    	MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, CountMapper.class);
        //mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job2.setReducerClass(CountReducer.class);
        job2.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class GenTieInfoMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    	
    	private Text outputKey = new Text();
    	private Map<String, String> map = new HashMap<String, String>();
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
        		map.clear();
	//        	url,pdocid,docid,发帖用户id,跟帖时间，跟帖id
	        	String[] strs = value.toString().split(",");
	        	String url = strs[0];
	        	String uid = strs[3];
	        	
	        	String channel = NeteaseChannel_CS.getChannelName(url);
	        	
	        	map.put("uid", uid);
	        	map.put("channel", channel);
	        	
	        	if(NeteaseContentType.special.match(url)){
	        		map.put("special", "true");
	        	}else{
	        		map.put("special", "false");
	        	}
	        	
	        	outputKey.set(JsonUtils.toJson(map));
	        	context.write(outputKey, NullWritable.get());
			} catch (Exception e) {
				context.getCounter("GenTieInfoMapper", "mapException").increment(1);
			}
        }
    }
    
	public static class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		
		private Text outputKey = new Text();
		
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        	
        	Map<String, String> map = JsonUtils.json2Map(key.toString());
        	
        	if("true".equals(map.get("special"))){
        		outputKey.set(map.get("channel") + "_special");
        		context.write(outputKey, NullWritable.get());
        	}
        	
        	outputKey.set(map.get("channel"));
        	context.write(outputKey, NullWritable.get());
        }
	}
	
    public static class CountMapper extends Mapper<Text, NullWritable, Text, IntWritable> {
    	
    	private IntWritable outputValue = new IntWritable(1);
    	
        @Override
        public void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
        	context.write(key, outputValue);
        }
    }
    
	public static class CountReducer extends Reducer<Text, IntWritable, Text, Text> {
		
		private Text outputValue = new Text();
		private Map<String, String> map = new HashMap<String, String>();
		
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	map.clear();
        	int sum = 0;
        	for(IntWritable val : values){
        		sum += val.get() ;
        	}
        	map.put("genTieUvSum", String.valueOf(sum));
        	
        	outputValue.set(JsonUtils.toJson(map));
        	context.write(key, outputValue);
        }
	}
}












