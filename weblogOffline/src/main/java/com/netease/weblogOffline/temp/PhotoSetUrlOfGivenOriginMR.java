package com.netease.weblogOffline.temp;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.LongPhotoSetWritable;
import com.netease.weblogOffline.data.PhotoSetWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 *
 * */
public class PhotoSetUrlOfGivenOriginMR extends MRJob {
	@Override
	public boolean init(String date) {
	    	
			List<String> dateList;
			try {
				String firstDay = DateUtils.getTheDayBefore(date, 69);
				dateList = DateUtils.getDateList(firstDay, date);
			} catch (ParseException e) {
				return false;
			}
			for (String d : dateList) {
				inputList.add(DirConstant.PHOTOSET_INCR+d);
			}
    		//输出列表
    		outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR+"PhotoSetUrlOfGivenOrigin/");
            return true;

	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());
    	
  	  for (String input : inputList) {
          Path path = new Path(input);
          if (getHDFS().exists(path)) {
              MultipleInputs.addInputPath(job, path, TextInputFormat.class, PhotoSetIncrMapper.class);
          }
	  }
    	
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        
        //reducer
        job.setReducerClass(MergeReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class PhotoSetIncrMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    	
    	private Text outputKey = new Text();
    
    	
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
        			String source = strs[8];
      			    
        			String url = strs[16];
      			if (source.equals("CFP")||source.equals("getty")||source.equals("东方IC")||source.equals("视觉中国")){
      				outputKey.set(source+"\t"+url);
      				
      				context.write(outputKey, NullWritable.get());
      			}
  	  	        
        		}else{
        			context.getCounter("PhotoSetIncrMapper", "parseLineError").increment(1);
        		}
			} catch (Exception e) {
				context.getCounter("PhotoSetIncrMapper", "mapException").increment(1);
			}
        }
    }
    
    
	public static class MergeReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

             context.write(key, NullWritable.get());

        }
	}
}












