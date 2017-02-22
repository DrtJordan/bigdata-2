package com.netease.weblogOffline.statistics.contentscore;

import java.io.IOException;
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
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.YcInfo;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 更新原创标题的全量文件
 * */
public class DailyYcInfoMergeMR extends MRJob {
	@Override
	public boolean init(String date) {
	    try {
            String theDayBefore = DateUtils.getTheDayBefore(date, 1);
            
            //输入列表
    		inputList.add(DirConstant.YC_INFO_INCR + date);
    		inputList.add(DirConstant.YC_INFO_ALL + theDayBefore);
    		
    		//输出列表
    		outputList.add(DirConstant.YC_INFO_ALL + date);
    		
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

    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), TextInputFormat.class, YcInfoIncrMapper.class);
    	MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, YcInfoAllMapper.class);
    	
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(YcInfo.class);
        
        //reducer
        job.setReducerClass(MergeReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(YcInfo.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class YcInfoIncrMapper extends Mapper<LongWritable, Text, Text, YcInfo> {
    	
    	private Text outputKey = new Text();
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
        		String line = value.toString();
        		//lmodify  url  title  ptime
        		String[] strs = line.split("\t");
        		if(strs.length >= 4){
        		  	YcInfo ycInfo = new YcInfo();
        		  	ycInfo.setLmodify(strs[0]);
        		  	ycInfo.setUrl(strs[1]);
        		  	ycInfo.setTitle(strs[2]);
        		  	ycInfo.setPtime(strs[3]);
        		  	if(strs.length >= 5){
        		  		ycInfo.setAuthor(strs[4]);
        		  	}
        		  	
        		  	outputKey.set(ycInfo.getUrl());
        		  	context.write(outputKey, ycInfo);
        		}else{
        			context.getCounter("YcTitleIncrMapper", "parseLineError").increment(1);
        		}
			} catch (Exception e) {
				context.getCounter("YcTitleIncrMapper", "mapException").increment(1);
			}
        }
    }
    
    public static class YcInfoAllMapper extends Mapper<Text, YcInfo, Text, YcInfo> {
        @Override
        public void map(Text key, YcInfo value, Context context) throws IOException, InterruptedException {
        	context.write(key, value);
        }
    }
    
	public static class MergeReducer extends Reducer<Text, YcInfo, Text, YcInfo> {
		
        @Override
        protected void reduce(Text key, Iterable<YcInfo> values, Context context) throws IOException, InterruptedException {
        	YcInfo latestYcInfo = null;
        	
        	for(YcInfo val : values){
        		if(latestYcInfo == null 
        				|| DateUtils.toLongTime(latestYcInfo.getLmodify(), "yyyy-MM-dd HH:mm:ss") < DateUtils.toLongTime(val.getLmodify(), "yyyy-MM-dd HH:mm:ss")){
        			latestYcInfo = new YcInfo(val);
        			latestYcInfo.setLmodify(val.getLmodify());
        		}
        	}
        	if(null != latestYcInfo){
        		context.write(key, latestYcInfo);
        	}
        }
	}
}












