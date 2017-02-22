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
public class ComparePathWith2MethodsMR extends MRJob {


    @Override 
    public boolean init(String date) {
        inputList.add(DirConstant.PATH_MIDLAYER_DIR + "pathrecordwithtime/" + date);
        inputList.add(DirConstant.PATH_MIDLAYER_DIR + "pathrecordwithpgr/" + date);
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "compareresultwith2methods/" + date);
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "compareresultwith2methods2/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;

        //1.比较两种方法下的路径统计是否一致，一致：输出路径值，计数为1 不一致：输出两条路径值，计数为0
        Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");

        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, TimeMethodMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, PgrMethodMapper.class);
        //mapperr
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        //reducer
        job.setReducerClass(ComparePathWith2MethodsReducer.class);
        //job.setCombinerClass(ComparePathWith2MethodsCombiner.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        //2.计算路径不一致所占百分比，用于判断时间排序的方法与pgr匹配的方法一致性的高低
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-2");

        MultipleInputs.addInputPath(job1, new Path(outputList.get(0)), SequenceFileInputFormat.class, ConsistencyCalculationMapper.class);
        
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job1.setReducerClass(ConsistencyCalculationReducer.class);
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

    
    public static class TimeMethodMapper extends Mapper<Text, Text, Text, Text> {
    	private Text outputvalue = new Text();
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
        	try{
        		String tempv = null;
        		tempv = "time" + value.toString();
        		outputvalue.set(tempv);
	            context.write(key, outputvalue);
        	}catch(Exception e){
        		context.getCounter("TimeMethodMapper", "result").increment(1);
        	}
        }
    }
    
    //将来自于pgr匹配的方式的文件夹中的记录的最后两个字段去掉，并在整条路径前加上pgr标识，以用于为reducer中区别做准备
    public static class PgrMethodMapper extends Mapper<Text, Text, Text, Text> {
    	private Text outputvalue = new Text();
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
        		respath = "pgr" + respath;
        		outputvalue.set(respath);
	            context.write(key, outputvalue);
        	}catch(Exception e){
        		context.getCounter("PgrMethodMapper", "result").increment(1);
        	}
        }
    }
    
    public static class ComparePathWith2MethodsReducer extends Reducer<Text,Text,Text,Text> {
    	private Text outputvalue = new Text();
    	
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
    		try{
    			String pathwithtime = null;
    			String pathwithpgr = null;
    			String tempv = null;
    			for(Text value : values){
    				tempv = value.toString();
    				if(tempv.contains("time")){
    					pathwithtime = tempv.replace("time", "");
    				}else if(tempv.contains("pgr")){
    					pathwithpgr = tempv.replace("pgr", "");
    				}
    			}
    			try{
	    			if(pathwithtime.equals(pathwithpgr)){
	    				outputvalue.set(pathwithtime + ",1");
	    				context.write(key, outputvalue);
	    			}else{
	    				outputvalue.set(pathwithtime + "&" + pathwithpgr + ",0");
	    				context.write(key, outputvalue);
	    			}
    			
	    		}catch(Exception e){
	    			context.getCounter("comparereslut", "result").increment(1);
	    		}
	    		//context.write(key,outputvalue);
    		}catch(Exception e){
    			context.getCounter("ComparePathWith2MethodsReducer", "result").increment(1);
    		}
    	}
    	
    	
    }
    
    public static class ComparePathWith2MethodsCombiner extends Reducer<Text,Text,Text,Text> {
    	public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException{
    		context.write(key, value);
    	}
    }
    
    public static class ConsistencyCalculationMapper extends Mapper<Text, Text, Text, IntWritable> {
    	private Text outputkey = new Text();
    	private IntWritable outputvalue = new IntWritable(1);
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
        	try{
        		String tempv = null;
        		String[] tempvalue = null;
        		
        		tempv = value.toString();
        		tempvalue = tempv.split(",");
        		
        		outputkey.set(tempvalue[tempvalue.length - 1]);
        		context.write(outputkey, outputvalue);
        	
        	}catch(Exception e){
        		context.getCounter("ConsistencyCalculationMapper", "result").increment(1);
        	}
        }
    }
    
    public static class ConsistencyCalculationReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    	private IntWritable outputvalue = new IntWritable();
    	private Text outputkey = new Text();
    	@Override
    	public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException{
    		try{
    			String tempk = null;
    			int sum = 0;
    			
    			tempk = key.toString();
                for(IntWritable value : values){
                    sum += value.get();
                }
                
                if(tempk.equals("1")){
                	outputkey.set("Consistent Path Amount");
                }else{
                	outputkey.set("Not Consistent Path Amount");
                }
                
                outputvalue.set(sum);
                context.write(outputkey, outputvalue);
    		}catch(Exception e){
    			context.getCounter("ConsistencyCalculationReducer", "result").increment(1);
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
