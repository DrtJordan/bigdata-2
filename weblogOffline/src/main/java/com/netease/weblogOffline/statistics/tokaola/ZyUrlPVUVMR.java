package com.netease.weblogOffline.statistics.tokaola;


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
import org.slf4j.LoggerFactory;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.logparsers.ZyLogParams;
import com.netease.weblogCommon.logparsers.ZylogParser;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.utils.HadoopUtils;


/**
 * zyownurlpvuv
 *
 */

public class ZyUrlPVUVMR extends MRJob {
	
	private static final org.slf4j.Logger log = LoggerFactory.getLogger(ZyUrlPVUVMR.class);

	@Override
	public boolean init(String date) {
        //输入列表
        inputList.add(DirConstant.ZY_LOG + date);
        
    	outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "zyownurlPvUvTemp/" + date);
        //输出列表
        outputList.add(DirConstant.WEBLOG_STATISTICS_DIR+"result_other/zyownurlpvuv/" + date);
 

		return true;
	}

    
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");


    	MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), TextInputFormat.class, LogMapper.class);
        
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        
        job1.setCombinerClass(PvUvTempReducer.class);
        
        //reducer
        job1.setReducerClass(PvUvTempReducer.class);
        job1.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
    	Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step2");

    	MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, PvUvMapper.class);
        
        //mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job2.setReducerClass(PvUvReducer.class);
        job2.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    	
    	private LogParser logParser = new ZylogParser(); 
    	
    	private Text outputkey = new Text();
    	private IntWritable outputValue = new IntWritable(1);
    	private Map<String, String> map = new HashMap<String, String>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	map.clear();
        	String oneLog = value.toString();
        	try {
				Map<String, String> logMap = logParser.parse(oneLog);
				String url = logMap.get(ZyLogParams.url);
				
				
				if(url.indexOf("own=")!=-1){

					map.put("url", url);
					map.put("uid", logMap.get(ZyLogParams.uid));
					
					outputkey.set(JsonUtils.toJson(map));
					context.write(outputkey, outputValue);
				}
			} catch (Exception e) {
				context.getCounter("LogMapper", "mapException").increment(1);
			}
        }
       
    }
    
    public static class PvUvTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
    	private IntWritable outputValue = new IntWritable();
    	
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum = 0;
        	
        	for(IntWritable val : values){
        		sum += val.get();
        	}
        	
        	outputValue.set(sum);
			context.write(key, outputValue);
        }
    }
    
    public static class PvUvMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
    	private Text outputKey = new Text();
    	
        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        	Map<String,String> map = JsonUtils.json2Map(key.toString());
        	outputKey.set(map.get("url"));
        	context.write(outputKey, value);
        }
    }
    
    public static class PvUvReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    	
    	private Text outputValue = new Text();
    	private Map<String,Integer> map = new HashMap<String, Integer>();
    	
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	map.clear();
        	int pv = 0;
        	int uv = 0;
        	
        	for(IntWritable val : values){
        		pv += val.get();
        		uv++;
        	}
			
			context.write(new Text(key.toString()+","+pv+","+uv), NullWritable.get());
        }
    }
    	  
	  
}


