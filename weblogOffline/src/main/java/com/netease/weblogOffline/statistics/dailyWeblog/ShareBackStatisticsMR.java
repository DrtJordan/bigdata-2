package com.netease.weblogOffline.statistics.dailyWeblog;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.utils.HadoopUtils;

public class ShareBackStatisticsMR extends MRJob {
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "shareBackCount/" + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_OTHER_DIR + "shareBackRes/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());
        	
        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, SBMapper.class);
        
        
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job.setReducerClass(CounterReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class SBMapper extends Mapper<Text, Text, Text, IntWritable> {
    	
    	private Text outputkey = new Text();
    	private IntWritable outputValue = new IntWritable();
    	
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	Map<String, String> map = JsonUtils.json2Map(value.toString());
        	String channel = map.get("channel");
        	String type = map.get("type");
        	String keyPrefix = channel + "_" + type + "_" ;
        	int s_sum = 0;
        	int b_sum = 0;
        	for(Entry<String, String> entry : map.entrySet()){
        		try {
					if(entry.getKey().startsWith("s_") || entry.getKey().startsWith("b_")){
						outputkey.set(keyPrefix + entry.getKey()); 

						int count = Integer.parseInt(entry.getValue());
						outputValue.set(count);
						if(entry.getKey().startsWith("s_")){
							s_sum += count;
						}else if(entry.getKey().startsWith("b_")){
							b_sum += count;
						}
						
						context.write(outputkey, outputValue);
					}
				} catch (Exception e) {
					context.getCounter("SBMapper", "mapError").increment(1);
				}
        	}
        	
        	if(0 != s_sum){
        		outputValue.set(s_sum);
        		
            	outputkey.set(channel + "_" + type + "_s_all");
    			context.write(outputkey, outputValue);
    			
            	outputkey.set(channel + "_all_s_all");
    			context.write(outputkey, outputValue);
        		
            	outputkey.set("all_all_s_all");
    			context.write(outputkey, outputValue);
        	}
        	if(0 != b_sum){
        		outputValue.set(b_sum);
        		
        	 	outputkey.set(channel + "_" + type + "_b_all");
    			context.write(outputkey, outputValue);
    			
            	outputkey.set(channel + "_all_b_all");
    			context.write(outputkey, outputValue);
    			
            	outputkey.set("all_all_b_all");
    			context.write(outputkey, outputValue);
        	}
        }
    }
    
    public static class CounterReducer extends Reducer<Text, IntWritable, Text, Text> {
    	
    	private Text outputValue = new Text();
    	
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum = 0;
        	int c = 0;
        	for(IntWritable value : values){
        		sum += value.get();
        		c++;
        	}
        	outputValue.set(sum + "\t" + c);
        	context.write(key, outputValue);
        }
    }
}












