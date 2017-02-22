package com.netease.weblogOffline.statistics.bigdatahouse;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.HashMapStringStringWritable;
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
    		inputList.add(DirConstant.HOUSE_URL_MEDIA_ALL + theDayBefore);
    		
    		//输出列表
    		outputList.add(DirConstant.HOUSE_URL_MEDIA_ALL + date);
    		
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
        job.setMapOutputValueClass(HashMapStringStringWritable.class);
        
        //reducer
        job.setReducerClass(MergeReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(HashMapStringStringWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class UrlMediaIncrMapper extends Mapper<LongWritable, Text, Text, HashMapStringStringWritable> {

    	Text outKey = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
        	
            	  HashMapStringStringWritable outputValue =MediaUtils.logParser(value);
        		//url title dkeys topicname source
	            if(outputValue!=null){
           	       InputSplit split = context.getInputSplit();
           	       Class<? extends InputSplit> splitClass = split.getClass();
           	       
           	        FileSplit fileSplit = null;
           	          if (splitClass.equals(FileSplit.class)) {
           	        fileSplit = (FileSplit) split;
           	          } else if (splitClass.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
           	 
           	     
           	      Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
           	      getInputSplitMethod.setAccessible(true);
           	      fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
           	          }
           	       Path path =fileSplit.getPath();
           	       String date = path.getParent().getName();
           	       
           	      outputValue.getHm().put("date", date);
           	      outputValue.getHm().put("flag", "Incr");
           	      outKey.set(outputValue.getHm().get("url"));
        		  context.write(outKey, outputValue);

        		}else{
        			context.getCounter("UrlMediaIncrMapper", "parseLineError").increment(1);
        		}
			} catch (Exception e) {
				context.getCounter("UrlMediaIncrMapper", "mapException").increment(1);
			}
        }
    }
    
    public static class UrlMediaAllMapper extends Mapper<Text, HashMapStringStringWritable, Text, HashMapStringStringWritable> {
    	Text outKey = new Text();
        @Override
        public void map(Text key, HashMapStringStringWritable value, Context context) throws IOException, InterruptedException {
        	
        	value.getHm().put("flag", "All");
        	context.write(key, value);
        }
    }
    
	public static class MergeReducer extends Reducer<Text, HashMapStringStringWritable, Text, HashMapStringStringWritable> {
		
		
        @Override
        protected void reduce(Text key, Iterable<HashMapStringStringWritable> values, Context context) throws IOException, InterruptedException {
	
        	HashMapStringStringWritable hmss = null;
        	for(HashMapStringStringWritable value : values){
        		if ((value.getHm().get("flag").equals("All")&&hmss==null)||value.getHm().get("flag").equals("Incr")){
        			hmss = new HashMapStringStringWritable(new HashMap<String,String>(value.getHm()));
        		}
        		
        	}
    		try {
    			URL url = new URL(key.toString());
    			hmss.getHm().remove("flag");
    			context.write(key, hmss);
			} catch (Exception e) {
				context.getCounter("MergeReducer", "illegal_url").increment(1);
			}
        }
	}
}












