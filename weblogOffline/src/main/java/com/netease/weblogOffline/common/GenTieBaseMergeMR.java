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
import com.netease.weblogOffline.data.GenTieBaseWritable;
import com.netease.weblogOffline.data.LongStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 更新GenTieBase全量文件
 * */
public class GenTieBaseMergeMR extends MRJob {
	@Override
	public boolean init(String date) {
	    try {
            String theDayBefore = DateUtils.getTheDayBefore(date, 1);
            
            //输入列表
    		inputList.add(DirConstant.GENTIE_BASE_INCR + date);
    		inputList.add(DirConstant.GENTIE_BASE_ALL + theDayBefore);
    		
    		//输出列表
    		outputList.add(DirConstant.GENTIE_BASE_ALL + date);
    		
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

    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), TextInputFormat.class, GenTieBaseMapper.class);
    	MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, GenTieBaseAllMapper.class);
    	
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongStringWritable.class);
        
        //reducer
        job.setReducerClass(MergeReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GenTieBaseWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class GenTieBaseMapper extends Mapper<LongWritable, Text, Text, LongStringWritable> {
    	
    	private Text outputKey = new Text();
    	private LongStringWritable outputValue = new LongStringWritable();
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
  
        		String line = value.toString();
        		//url，pdocid（专题的），docid（文章的唯一编号） （逗号分隔）
        		int index = line.indexOf("http://");
        		if (index!=-1){
        			line= line.substring(index);
        			
        			while(line.contains(",,")){
        				line = line.replace(",,", ",(null),");
        			}
            		if (line.endsWith(",")){
            			line= line+"(null)";
            		}
            		String[] str = line.split(",");
            		if (str.length==3&&(!"http://e.163.com/null".equals(str[0]))){
            			String url = str[0];
            			outputKey.set(url);
            			outputValue.setFirst(1);
            			outputValue.setSecond(line);
            			
            			context.write(outputKey, outputValue);	
            		}else if (str.length==4){
            			String url = str[0]+"-"+str[1];
            			line = url+","+str[2]+","+str[3];
            			
            			outputKey.set(url);
            			outputValue.setFirst(1);
            			outputValue.setSecond(line);
            			context.write(outputKey, outputValue);	
            		}else{
            			context.getCounter("GenTieBaseMapper", "parseLineError").increment(1);
            		}
        			
        		}else {
        			context.getCounter("GenTieBaseMapper", "SqlDirtyData").increment(1);
        		}
        	}
    }
    
    public static class GenTieBaseAllMapper extends Mapper<Text, GenTieBaseWritable, Text, LongStringWritable> {
    	private LongStringWritable outputValue = new LongStringWritable();
        @Override
        public void map(Text key, GenTieBaseWritable value, Context context) throws IOException, InterruptedException {
        	outputValue.setFirst(0);
        	outputValue.setSecond(value.toString());
        	context.write(key, outputValue);
        }
    }
    
	public static class MergeReducer extends Reducer<Text, LongStringWritable, Text, GenTieBaseWritable> {
		
		private GenTieBaseWritable gbw = new GenTieBaseWritable();
		
        @Override
        protected void reduce(Text key, Iterable<LongStringWritable> values, Context context) throws IOException, InterruptedException {
        	String genTieBaseInfo = null;
        	for(LongStringWritable lsw : values){
        		if((0 == lsw.getFirst() && null == genTieBaseInfo) 
        				|| 1 == lsw.getFirst()){
        			genTieBaseInfo = lsw.getSecond();
        		}
        	}
        	String[] strs = genTieBaseInfo.split(",");
        	gbw.setUrl(strs[0]);
        	gbw.setPdocid(strs[1]);
        	gbw.setDocid(strs[2]);
        	

			try {
				URL url = new URL(key.toString());
				context.write(key, gbw);
			} catch (Exception e) {
				context.getCounter("MergeReducer", "illegal_url").increment(1);
			}
        }
	}
}












