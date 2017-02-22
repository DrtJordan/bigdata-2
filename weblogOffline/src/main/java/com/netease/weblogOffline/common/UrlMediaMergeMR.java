package com.netease.weblogOffline.common;

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogOffline.data.LongStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 更新url-media全量文件
 * */
public class UrlMediaMergeMR extends MRJob {
	@Override
	public boolean init(String date) {
	    try {
            String theDayBefore = DateUtils.getTheDayBefore(date, 1);
            
            //输入列表
    		inputList.add(DirConstant.URL_MEDIA_INCR + date);
    		inputList.add(DirConstant.URL_MEDIA_ALL + theDayBefore);
    		
    		//输出列表
    		outputList.add(DirConstant.URL_MEDIA_ALL + date);
    		
            return true;
        } catch (ParseException e) {
            LOG.error(e);
            return false;
        }
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());

    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), TextInputFormat.class, UrlMediaIncrMapper.class);
    	MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, UrlMediaAllMapper.class);
    	
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongStringWritable.class);
        
        //reducer
        job.setReducerClass(MergeReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class UrlMediaIncrMapper extends Mapper<LongWritable, Text, Text, LongStringWritable> {
    	
    	private Text outputKey = new Text();
    	private LongStringWritable outputValue = new LongStringWritable();
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
        		String line = value.toString();
        		//url title dkeys topicname source
        		String[] strs = line.split("\t");
        		if(strs.length >= 5){
        			String url = strs[0];
        			String source = strs[strs.length - 1];
        			String[] ss = source.split("&&&");
//        			if(ss.length >= 1){
    				outputKey.set(url);
    				String media = ss[0];
        		  	outputValue.setFirst(1);
        		  	outputValue.setSecond(media);
        		  	context.write(outputKey, outputValue);
//        			}else{
//        				context.getCounter("UrlMediaIncrMapper", "parseMediaError").increment(1);
//        			}
        		}else{
        			context.getCounter("UrlMediaIncrMapper", "parseLineError").increment(1);
        		}
			} catch (Exception e) {
				context.getCounter("UrlMediaIncrMapper", "mapException").increment(1);
			}
        }
    }
    
    public static class UrlMediaAllMapper extends Mapper<Text, Text, Text, LongStringWritable> {
    	private LongStringWritable outputValue = new LongStringWritable();
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	outputValue.setFirst(0);
        	outputValue.setSecond(value.toString());
        	context.write(key, outputValue);
        }
    }
    
	public static class MergeReducer extends Reducer<Text, LongStringWritable, Text, Text> {
		
		private Text outputValue = new Text();
		
        @Override
        protected void reduce(Text key, Iterable<LongStringWritable> values, Context context) throws IOException, InterruptedException {
        	String media = null;
        	for(LongStringWritable lsw : values){
        		if((0 == lsw.getFirst() && null == media) 
        				|| 1 == lsw.getFirst()){
        			media = lsw.getSecond();
        		}
        	}
			outputValue.set(media);

			try {
				URL url = new URL(key.toString());
				context.write(key, outputValue);
			} catch (Exception e) {
				context.getCounter("MergeReducer", "illegal_url").increment(1);
			}
        }
	}
}












