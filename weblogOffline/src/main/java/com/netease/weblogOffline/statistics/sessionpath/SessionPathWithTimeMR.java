package com.netease.weblogOffline.statistics.sessionpath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.StringStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 *
 * 1、计算用户在一个session中从入口页（入口页的来源可能是外站，也可能是163Home）到后续页面路径统计
 * 
 * Created by ruihuang on 2015/5/26.
 */
public class SessionPathWithTimeMR extends MRJob {


    @Override 
    public boolean init(String date) {
        inputList.add(DirConstant.PATH_MIDLAYER_DIR + "sessionpathentry/" + date);
        inputList.add(DirConstant.PATH_MIDLAYER_DIR + "sessionpathnotentry/" + date);
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "pathrecordwithtime/" + date);
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "pathrecordwithtime2/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;

        //1.利用时间排序的方法获得path,包括ref页
        Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");

        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, EntryWithTimeMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, NotEntryWithTimeMapper.class);
        //mapperr
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        //reducer
        job.setReducerClass(FromRefWithTimeReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        //2.统计具有相同访问路径的量有多少（暂定不同session来自不同用户）
        Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-2");
        
        MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, PathRecordWithTimeMapper.class);
        //mapperr
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job2.setCombinerClass(PathRecordWithTimeReducer.class);
        job2.setReducerClass(PathRecordWithTimeReducer.class);
        job2.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
       
        return jobState;
    }

    
    public static class EntryWithTimeMapper extends Mapper<Text, Text, Text, Text> {
    	private Text outputvalue = new Text();
    	private String resvalue = null;
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
        	try{
	        	resvalue = "entry" + value.toString();
	        	outputvalue.set(resvalue);
	        
	            context.write(key, outputvalue);
        	}catch(Exception e){
        		context.getCounter("EntryWithTimeMapper", "outputvalue").increment(1);
        	}
        }
    }
    
    public static class NotEntryWithTimeMapper extends Mapper<Text, Text, Text, Text> {
    	
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
        	try{
	            context.write(key, value);
        	}catch(Exception e){
        		context.getCounter("NotEntryWithTimeMapper", "outputvalue").increment(1);
        	}
        }
    }
    
    public static class FromRefWithTimeReducer extends Reducer<Text,Text,Text,Text> {
    	private Text outputvalue = new Text();
    	private List<String> timelist = new ArrayList<String>();
    	private Map<String,String> timeurlpair = new HashMap<String,String>();
    	
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
    		try{
    			String[] tempvalue = null;
    			String respath = null;
	    		timelist.clear();
	    		timeurlpair.clear();
	    		
	    		for(Text value : values){
	    			String tempv = null;
	    			tempv = value.toString();
	    			
	    			if(tempv.contains("entry")){
	    				tempvalue = tempv.replace("entry", "").split(",");
	    				timeurlpair.put(tempvalue[3], tempvalue[0]);
	    				timelist.add(tempvalue[3]);
	    				respath = tempvalue[1] + "," + filterUrl(tempvalue[0]);
	    				
	    			}else{
	    				tempvalue = tempv.split(",");
	    				timeurlpair.put(tempvalue[3], tempvalue[0]);
	    				timelist.add(tempvalue[3]);
	    			}
	    		}
	    		//按时间排序
	    		Collections.sort(timelist);
	    		
	    		//按时间顺序查找对应的url分类，并且解析该url
	    		for(int i = 0; i < timelist.size(); i++){
	    			if(i == 0 && respath.isEmpty()){
	    				respath = filterUrl(timeurlpair.get(timelist.get(i)));
	    			}
	    			else if(i == 0 && !respath.isEmpty()){
	    				respath = respath;
	    			}
	    			else if(i != 0){
	    				respath = respath + "," + filterUrl(timeurlpair.get(timelist.get(i)));
	    				
	    			}
	    		}
	    		
	    		try{
	    			outputvalue.set(respath);
	    		}catch(Exception e){
	    			context.getCounter("outputvalue", "outputvalue").increment(1);
	    		}
	    		context.write(key,outputvalue);
    		}catch(Exception e){
    			context.getCounter("FromRefWithTimeReducer", "outputvalue").increment(1);
    		}
    	}
    	
    	
    }
    
    public static class PathRecordWithTimeMapper extends Mapper<Text, Text, Text, IntWritable> {
    	private IntWritable outputvalue = new IntWritable(1);
    	
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
        	try{
	            context.write(value, outputvalue);
        	}catch(Exception e){
        		context.getCounter("PathRecordTimeMapper", "outputvalue").increment(1);
        	}
        }
    }

    public static class PathRecordWithTimeReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    	private IntWritable outputvalue = new IntWritable();
    	
    	@Override
    	public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException{
    		try{
    			int sum = 0;
                for(IntWritable value : values){
                    sum += value.get();
                }
                outputvalue.set(sum);
                context.write(key, outputvalue);
    		}catch(Exception e){
    			context.getCounter("PathRecordWithTimeReducer", "outputvalue").increment(1);
    		}
    		
    	}
    }
    
    
    /**
     * 去掉www或者com的后缀
     * @param URL
     * @return
     */
    private static String filterUrl(String URL){
        String filterurl = URL==null? null: URL.toLowerCase();
        if(filterurl!=null && filterurl.length()>0){
            if(filterurl.endsWith(".")){
                filterurl = filterurl.substring(0, filterurl.length()-1);
            }
            if(filterurl.contains(".cz")
                || filterurl.contains(".com")
                || filterurl.contains(".cn")
                || filterurl.contains(".org")
                || filterurl.contains(".edu")
                || filterurl.contains(".net")){

                filterurl = getFilterUrl(filterurl, ".cz");
                filterurl = getFilterUrl(filterurl, ".com");
                filterurl = getFilterUrl(filterurl, ".cn");
                filterurl = getFilterUrl(filterurl, ".org");
                filterurl = getFilterUrl(filterurl, ".edu");
                filterurl = getFilterUrl(filterurl, ".net");
                if(filterurl.contains(".")){
                    filterurl = filterurl.substring(filterurl.lastIndexOf(".")+1);
                }
            }
        }
        return filterurl;
    }

    private static String getFilterUrl(String filterurl, String rule) {
        if(filterurl.endsWith(rule)){
            filterurl = filterurl.substring(0, filterurl.lastIndexOf(rule));
        }
        return filterurl;
    }

    
    
   
}
