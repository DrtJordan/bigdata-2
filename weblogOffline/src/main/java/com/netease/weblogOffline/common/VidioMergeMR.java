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
import com.netease.weblogOffline.data.LongVidioWritable;
import com.netease.weblogOffline.data.VidioWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 更新photoSet全量文件
 * */
public class VidioMergeMR extends MRJob {
	@Override
	public boolean init(String date) {
	    try {
            String theDayBefore = DateUtils.getTheDayBefore(date, 1);
            
            //输入列表
    		inputList.add(DirConstant.VIDIO_INCR + date);
    		inputList.add(DirConstant.VIDIO_ALL + theDayBefore);
    		
    		//输出列表
    		outputList.add(DirConstant.VIDIO_ALL + date);
    		
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

    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), TextInputFormat.class, VidioIncrMapper.class);
    	MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, VidioAllMapper.class);
    	
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongVidioWritable.class);
        
        //reducer
        job.setReducerClass(MergeReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VidioWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class VidioIncrMapper extends Mapper<LongWritable, Text, Text, LongVidioWritable> {
    	
    	private Text outputKey = new Text();
    	private VidioWritable vw=new VidioWritable();
    	
    	private LongVidioWritable outputValue = new LongVidioWritable();
    	
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
        			String vId = strs[0];
        			outputKey.set(vId);
        		  	outputValue.setFirst(1); 
        			
        		  	vw.setvId(strs[0]);
        		  	vw.setUrl(strs[1]);
        		  	vw.setTitle(strs[2]);
        		  	vw.setTag(strs[3]);
        		  	vw.setOriginalAuthor(strs[4]);
        		  	vw.setUpPerson(strs[5]);
        		  	vw.setModifyPerson(strs[6]);
        		  	vw.setSource(strs[7]);
        		  	vw.setGentieId(strs[8]);
        		  	vw.setUpTime(strs[9]);
        		  	vw.setEditor(strs[10]);
        		  	vw.setLmodify(strs[11]); 	
        		  	outputValue.setSecond(vw);
        		  	context.write(outputKey, outputValue);
        		}else{
        			context.getCounter("PhotoSetIncrMapper", "parseLineError").increment(1);
        		}
			} catch (Exception e) {
				context.getCounter("PhotoSetIncrMapper", "mapException").increment(1);
			}
        }
    }
    
    public static class VidioAllMapper extends Mapper<Text, VidioWritable, Text, LongVidioWritable> {
    	private LongVidioWritable outputValue = new LongVidioWritable();
        @Override
        public void map(Text key, VidioWritable value, Context context) throws IOException, InterruptedException {
        	outputValue.setFirst(0);
        	outputValue.setSecond(value);
        	context.write(key, outputValue);
        }
    }
    
	public static class MergeReducer extends Reducer<Text, LongVidioWritable, Text, VidioWritable> {
		private VidioWritable vw ;
		
        @Override
        protected void reduce(Text key, Iterable<LongVidioWritable> values, Context context) throws IOException, InterruptedException {
    		try {
    			vw = null;
            	for(LongVidioWritable lsw : values){
            		if((0 == lsw.getFirst() && null == vw) 
            				|| 1 == lsw.getFirst()){
            			vw = lsw.getSecond();
            		}
            	}
    		
    		
             context.write(key, vw);
			} catch (Exception e) {
				context.getCounter("MergeReducer", "mapException").increment(1);
			}
        }
	}
}












