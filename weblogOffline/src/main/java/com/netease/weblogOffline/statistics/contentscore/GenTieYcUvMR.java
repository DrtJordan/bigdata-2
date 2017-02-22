package com.netease.weblogOffline.statistics.contentscore;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.YcInfo;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 *  通过跟帖的数据，计算当日每个频道跟帖uv、每个频道下专题跟帖uv
 * */
public class GenTieYcUvMR extends MRJob {
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.GEN_TIE_INFO + date);
		inputList.add(DirConstant.YC_INFO_ALL + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "genTieYcUvTemp/" + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "genTieYcUv/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");

    	MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), TextInputFormat.class, GenTieInfoMapper.class);
    	MultipleInputs.addInputPath(job1, new Path(inputList.get(1)), SequenceFileInputFormat.class, CmsYcInfoMapper.class);
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        
        //reducer
        job1.setReducerClass(GenTieYcMergeReducer.class);
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
        job2.setMapOutputValueClass(NullWritable.class);
        
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
    
    public static class GenTieInfoMapper extends Mapper<LongWritable, Text, Text, Text> {
    	
    	private Text outputKey = new Text();
    	private Text outputValue = new Text();
    	private Map<String, String> map = new HashMap<String, String>();
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
        		map.clear();
	//        	url,pdocid,docid,发帖用户id,跟帖时间，跟帖id
	        	String[] strs = value.toString().split(",");
	        	String url = strs[0];
	        	String uid = strs[3];
	        	
	        	map.put("uid", uid);
	        	
	        	outputKey.set(url);
	        	outputValue.set(JsonUtils.toJson(map));
	        	context.write(outputKey, outputValue);
			} catch (Exception e) {
				context.getCounter("GenTieInfoMapper", "mapException").increment(1);
			}
        }
    }
    
    public static class CmsYcInfoMapper extends Mapper<Text, YcInfo, Text, Text> {
    	private Text outputValue = new Text();
    	private Map<String,String> outputMap = new HashMap<String, String>();
    	
        @Override
        public void map(Text key, YcInfo value, Context context) throws IOException, InterruptedException {
        	outputMap.clear();
        	outputMap.put("source", "yc");
        	outputValue.set(JsonUtils.toJson(outputMap));
        	context.write(key, outputValue);
        }
    }
    
	public static class GenTieYcMergeReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		private Text outputKey = new Text();
		private Set<String> set = new HashSet<String> ();
    	private Map<String, String> valuesMap = new HashMap<String, String>();
		
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	set.clear();
        	boolean isYc = false;
        	String url = key.toString();
        	String channel = NeteaseChannel_CS.getChannelName(url);
        	
        	for(Text value : values){
        		Map<String, String> map = JsonUtils.json2Map(value.toString());
        		
        		if(map.containsKey("source")){
        			isYc = true;
        		}else if(map.containsKey("uid")) {
        			set.add(map.get("uid"));
        		}
        		
        		if(isYc){
        			if(set.size() > 0){
        				for(String uid : set){
        					valuesMap.clear();
        					valuesMap.put("uid", uid);
        					valuesMap.put("channel", channel);
        					outputKey.set(JsonUtils.toJson(valuesMap));
        					context.write(outputKey, NullWritable.get());
        				}
        				set.clear();
        			}else if(map.containsKey("uid")){
        				valuesMap.clear();
        				valuesMap.put("uid", map.get("uid"));
        				valuesMap.put("channel", channel);
    					outputKey.set(JsonUtils.toJson(valuesMap));
    					context.write(outputKey, NullWritable.get());
        			}
        		}
        	}
        }
	}
	
    public static class CountMapper extends Mapper<Text, NullWritable, Text, NullWritable> {
        @Override
        public void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
        	context.write(key, value);
        }
    }
    
	public static class CountReducer extends Reducer<Text, IntWritable, Text, Text> {
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private Map<String, Integer> counterMap = new HashMap<String, Integer>();
		
    	@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
    		for(Entry<String, Integer> e : counterMap.entrySet()){
    			Map<String, String> map = new HashMap<String, String>();
    			map.put("genTieUvSum", String.valueOf(e.getValue()));
    			
    			outputKey.set(e.getKey() + "_yc");
    			outputValue.set(JsonUtils.toJson(map));
    			context.write(outputKey, outputValue);
    		}
		}
		
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	Map<String, String> map = JsonUtils.json2Map(key.toString());
        	
        	String channel = map.get("channel");
        	
        	Integer oldSum = counterMap.get(channel);
        	if(null == oldSum){
        		oldSum = 0;
        	}
        	counterMap.put(channel, oldSum + 1);
        }
	}
}












