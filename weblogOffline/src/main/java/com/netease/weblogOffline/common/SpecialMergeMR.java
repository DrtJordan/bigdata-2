package com.netease.weblogOffline.common;

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
import com.netease.weblogOffline.data.LongSpecialWritable;
import com.netease.weblogOffline.data.SpecialWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 更新专题全量文件
 * */
public class SpecialMergeMR extends MRJob {
	@Override
	public boolean init(String date) {
	    try {
            String theDayBefore = DateUtils.getTheDayBefore(date, 1);
            
            //输入列表
    		inputList.add(DirConstant.SPECIAL_INCR + date);
    		inputList.add(DirConstant.SPECIAL_ALL + theDayBefore);
    		
    		//输出列表
    		outputList.add(DirConstant.SPECIAL_ALL + date);
    		
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

    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), TextInputFormat.class, SpecialIncrMapper.class);
    	MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, SpecialAllMapper.class);
    	
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongSpecialWritable.class);
        
        //reducer
        job.setReducerClass(MergeReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SpecialWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class SpecialIncrMapper extends Mapper<LongWritable, Text, Text, LongSpecialWritable> {
    	
    	private Text outputKey = new Text();
    	private SpecialWritable sw=new SpecialWritable();
    	
    	private LongSpecialWritable outputValue = new LongSpecialWritable();
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
        		String line = value.toString();
        		//
        		
        		while(line.contains("\t\t")){
    				line = line.replace("\t\t", "\t(null)\t");
    			}
        		if (line.endsWith("\t")){
        			line= line+"(null)";
        		}
        		
        		String[] strs = line.split("\t");
        		if(strs.length == 12){
        			
                    outputKey.set(strs[0]);
        		  	outputValue.setFirst(1); 
        		  	sw.setUrl(strs[0]);
        		  	sw.setCommentId(strs[1]);
        		  	sw.setChannel(strs[2]);    
        		  	sw.setCloumn(strs[3]);
        		  	sw.setEnglishName(strs[4]);
        		  	sw.setChineseName(strs[5]);
        		  	sw.setPageNumber(strs[6]);
        		  	sw.setSourceTag(strs[7]);
        		  	sw.setOriginalAuthor(strs[8]);
        		  	sw.setPublishTime(strs[9]);
        		  	sw.setEditor(strs[10]);
        		  	sw.setLmodify(strs[11]);
        		  	
        		  	outputValue.setSecond(sw);
        		  	context.write(outputKey, outputValue);
        		}else{
        			context.getCounter("PhotoSetIncrMapper", "parseLineError").increment(1);
        		}
			} catch (Exception e) {
				context.getCounter("PhotoSetIncrMapper", "mapException").increment(1);
			}
        }
    }
    
    public static class SpecialAllMapper extends Mapper<Text, SpecialWritable, Text, LongSpecialWritable> {
    	private LongSpecialWritable outputValue = new LongSpecialWritable();
        @Override
        public void map(Text key, SpecialWritable value, Context context) throws IOException, InterruptedException {
        	outputValue.setFirst(0);
        	outputValue.setSecond(value);
        	context.write(key, outputValue);
        }
    }
    
	public static class MergeReducer extends Reducer<Text, LongSpecialWritable, Text, SpecialWritable> {
		private SpecialWritable sw ;
		
        @Override
        protected void reduce(Text key, Iterable<LongSpecialWritable> values, Context context) throws IOException, InterruptedException {
    		try {
    			sw = null;
            	for(LongSpecialWritable lsw : values){
            		if((0 == lsw.getFirst() && null == sw) 
            				|| 1 == lsw.getFirst()){
            			sw = lsw.getSecond();
            		}
            	}
    		
    		
             context.write(key, sw);
			} catch (Exception e) {
				context.getCounter("MergeReducer", "mapException").increment(1);
			}
        }
	}
}












