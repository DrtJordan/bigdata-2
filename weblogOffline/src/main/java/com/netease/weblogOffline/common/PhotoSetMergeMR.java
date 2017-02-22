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
import com.netease.weblogOffline.data.LongPhotoSetWritable;
import com.netease.weblogOffline.data.PhotoSetWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 更新photoSet全量文件
 * */
public class PhotoSetMergeMR extends MRJob {
	@Override
	public boolean init(String date) {
	    try {
            String theDayBefore = DateUtils.getTheDayBefore(date, 1);
            
            //输入列表
    		inputList.add(DirConstant.PHOTOSET_INCR + date);
    		inputList.add(DirConstant.PHOTOSET_ALL + theDayBefore);
    		
    		//输出列表
    		outputList.add(DirConstant.PHOTOSET_ALL + date);
    		
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

    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), TextInputFormat.class, PhotoSetIncrMapper.class);
    	MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, PhotoSetAllMapper.class);
    	
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPhotoSetWritable.class);
        
        //reducer
        job.setReducerClass(MergeReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhotoSetWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class PhotoSetIncrMapper extends Mapper<LongWritable, Text, Text, LongPhotoSetWritable> {
    	
    	private Text outputKey = new Text();
    	private PhotoSetWritable ps=new PhotoSetWritable();
    	
    	private LongPhotoSetWritable outputValue = new LongPhotoSetWritable();
    	
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
        		if(strs.length == 20){
        			String setId = strs[0];
        			String channel =  strs[17];
        			outputKey.set(channel+"_"+setId);
        		  	outputValue.setFirst(1); 

        		  	ps.setSetId(strs[0]);
        		  	ps.setSetName(strs[1]);
        		  	ps.setTopicId(strs[2]);
        		  	ps.setCreateTime(strs[3]);
        		  	ps.setImgSum(strs[4]);
        		  	ps.setCover(strs[5]);
        		  	ps.setUserId(strs[6]);
        		  	ps.setPostId(strs[7]);
        		  	ps.setSource(strs[8]);
        		  	ps.setReporter(strs[9]);
        		  	ps.setNickName(strs[10]);
        		  	ps.setKeyWord(strs[11]);
        		  	ps.setFirstPublish(strs[12]);
        		  	ps.setClientSetName(strs[13]);
        		  	ps.setOriginalAuthor(strs[14]);
        		  	ps.setOriginalType(strs[15]);
        		  	ps.setSetUrl(strs[16]);
        		  	ps.setChannel(strs[17]);
        		  	ps.setEditor(strs[18]);
        		  	ps.setLmodify(strs[19]);
        		  	
        		  	outputValue.setSecond(ps);
        		  	context.write(outputKey, outputValue);
        		}else{
        			context.getCounter("PhotoSetIncrMapper", "parseLineError").increment(1);
        		}
			} catch (Exception e) {
				context.getCounter("PhotoSetIncrMapper", "mapException").increment(1);
			}
        }
    }
    
    public static class PhotoSetAllMapper extends Mapper<Text, PhotoSetWritable, Text, LongPhotoSetWritable> {
    	private LongPhotoSetWritable outputValue = new LongPhotoSetWritable();
        @Override
        public void map(Text key, PhotoSetWritable value, Context context) throws IOException, InterruptedException {
        	outputValue.setFirst(0);
        	outputValue.setSecond(value);
        	context.write(key, outputValue);
        }
    }
    
	public static class MergeReducer extends Reducer<Text, LongPhotoSetWritable, Text, PhotoSetWritable> {
		private PhotoSetWritable ps ;
		
        @Override
        protected void reduce(Text key, Iterable<LongPhotoSetWritable> values, Context context) throws IOException, InterruptedException {
    		try {
    			ps = null;
            	for(LongPhotoSetWritable lsw : values){
            		if((0 == lsw.getFirst() && null == ps) 
            				|| 1 == lsw.getFirst()){
            			ps = lsw.getSecond();
            		}
            	}
    		
    		
             context.write(key, ps);
			} catch (Exception e) {
				context.getCounter("MergeReducer", "mapException").increment(1);
			}
        }
	}
}












