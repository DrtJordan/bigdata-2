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
public class SessionPathWithPgrMR extends MRJob {


    @Override 
    public boolean init(String date) {
        inputList.add(DirConstant.PATH_MIDLAYER_DIR + "sessionpathentry/" + date);
        inputList.add(DirConstant.PATH_MIDLAYER_DIR + "sessionpathnotentry/" + date);
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "pathrecordwithpgr/" + date);
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "pathrecordwithpgr2/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;

        
        //1.按照pgr与prev_pgr的逻辑关系，统计用户访问路径
        Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-3");
        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, EntryWithTimeMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, NotEntryWithTimeMapper.class);
        //mapperr
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

      //reducer
        job.setReducerClass(FromRefWithPgrReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        //2.统计具有相同访问路径的量有多少（暂定不同session来自不同用户）
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-4");
        
        MultipleInputs.addInputPath(job1, new Path(outputList.get(0)), SequenceFileInputFormat.class, PathRecordWithPgrMapper.class);
        //mapperr
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job1.setCombinerClass(PathRecordWithPgrReducer.class);
        job1.setReducerClass(PathRecordWithPgrReducer.class);
        job1.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(1)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        if(!job1.waitForCompletion(true)){
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
    
    
    public static class FromRefWithPgrReducer extends Reducer<Text,Text,Text,Text> {
    	private Text outputvalue = new Text();
    	private Map<String,String> prevurlpgrtuple = new HashMap<String,String>();
    	
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
    		try{
    			prevurlpgrtuple.clear();
    			String respath = null;
    			String[] tempvalue = null;
    			String targetpgr = null;
    			String[] pgrurlpair = null;
    			int passrecord = 0, totalrecord = 0;
	    		for(Text value : values){
	    			String tempv = null;
	    			tempv = value.toString();
	    			if(tempv.contains("entry")){
	    				tempvalue = tempv.replace("entry", "").split(",");
	    				//tempvalue[2] == pgr, tempvalue[0] == url
	    				targetpgr = tempvalue[2];
	    				//tempvalue[1] == ref
	    				respath = tempvalue[1] + "," + filterUrl(tempvalue[0]);
	    				passrecord = 2;
	    			}else{
	    				tempvalue = tempv.split(",");
	    				totalrecord++;
	    				//tempvalue[1] == prev_pgr, tempvalue[2] == pgr, tempvalue[0] == url
	    				prevurlpgrtuple.put(tempvalue[1], tempvalue[2] + "," + tempvalue[0]);
	    			}
	    		}
	    		totalrecord = totalrecord + passrecord;
	    		
	    		for(int i = 0; i < prevurlpgrtuple.size(); i++){
	    			if(prevurlpgrtuple.containsKey(targetpgr)){
	    				pgrurlpair = prevurlpgrtuple.get(targetpgr).split(",");
	    				respath = respath + "," + filterUrl(pgrurlpair[1]);
	    				targetpgr = pgrurlpair[0];
	    				passrecord++;
	    				
	    			}else{
	    				break;
	    			}
	    		}
	    		//when calculate the lost numbers, not consider multiple same prev_pgrs
	    		//lostrecord = totalrecord - passreocrd
	    		//passrecord表明现有路径所跳转的网页数，lostrecord表明由于不同网页拥有同一条prev_pgr从而导致的断路问题所记录的丢失网页的数目
	    		respath = respath + "," + String.valueOf(passrecord) + "," + String.valueOf(totalrecord - passrecord);
	    		
	    		try{
	    			outputvalue.set(respath);
	    		}catch(Exception e){
	    			context.getCounter("respath", "outputvalue").increment(1);
	    		}
	    		context.write(key,outputvalue);
    		}catch(Exception e){
    			context.getCounter("FromRefWithPgrReducer", "outputvalue").increment(1);
    		}
    	}	
    }
    
    public static class PathRecordWithPgrMapper extends Mapper<Text, Text, Text, IntWritable> {
    	private IntWritable outputvalue = new IntWritable(1);
    	private Text outputkey = new Text();
    	private List<String> valuelist = new LinkedList<String>();
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
        	try{
        		valuelist.clear();
        		String[] tempvalue = null;
        		String respath = null;
        		tempvalue = value.toString().split(",");
        		for(int i = 0; i < tempvalue.length; i++){
        			valuelist.add(tempvalue[i]);
        		}
        		int length = valuelist.size();
        		valuelist.remove(length - 1);
        		valuelist.remove(length - 2);
        		respath = valuelist.get(0);
        		
        		for(int i = 1; i < valuelist.size(); i++){
        			respath = respath + "," + valuelist.get(i);
        		}
        		
        		outputkey.set(respath);
	            context.write(outputkey, outputvalue);
        	}catch(Exception e){
        		context.getCounter("PathRecordWithPgrMapper", "outputvalue").increment(1);
        	}
        }
    }

    public static class PathRecordWithPgrReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
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
    			context.getCounter("PathRecordWithPgrReducer", "outputvalue").increment(1);
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
