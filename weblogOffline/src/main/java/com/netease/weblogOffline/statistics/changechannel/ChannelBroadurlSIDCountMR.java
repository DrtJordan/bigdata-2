package com.netease.weblogOffline.statistics.changechannel;


import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.LoggerFactory;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.weblogfilter.WeblogFilterUtils;
import com.netease.weblogOffline.data.ThreeStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;



/**
 * ChannelBroadurl  sidCount
 *        
 */

public class ChannelBroadurlSIDCountMR extends MRJob {
	
	private static final org.slf4j.Logger log = LoggerFactory.getLogger(ChannelBroadurlSIDCountMR.class);

	@Override
	public boolean init(String date) {

		inputList.add(DirConstant.WEBLOG_FilterLOG
				+ date);
        //输出列表
        outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR +"ChannelBroadurlSIDCounttemp/" + date);
        outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR +"ChannelBroadurlSIDCount/" + date);

		return true;
	}

    
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName()+"temp1");


    	MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, LogMapper.class);
        
        //mapper
        job1.setMapOutputKeyClass(ThreeStringWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        
        
        //reducer
        job1.setReducerClass(SIDTempReducer.class);
        job1.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(ThreeStringWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
    	Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName()+"temp2");


    	MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, SIDMapper.class);
        
        //mapper
    	job2.setMapOutputKeyClass(ThreeStringWritable.class);
    	job2.setMapOutputValueClass(IntWritable.class);
        
    	job2.setCombinerClass(SIDReducer.class);
        
        //reducer
    	job2.setReducerClass(SIDReducer.class);
    	job2.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job2.setOutputKeyClass(ThreeStringWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        
        
        return jobState;
    }
    
    public static class LogMapper extends Mapper<NullWritable, Text, ThreeStringWritable, IntWritable> {
    	

    	
    	private IntWritable outputValue = new IntWritable(1);

        @Override
        public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {



        	try {
        		HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());
 
        		String url =lineMap.get("url");
        		String event = lineMap.get("event");
        		
				NeteaseChannel_CS nce = NeteaseChannel_CS.getChannel(url);
				url = url.replace("_", "");
				if(event.equals("launch")&&nce != null&&NeteaseContentType.artical.match(url)){
					String str[] = lineMap.get("project").split("@version@");
					String sid = lineMap.get("sid");
					if (str.length==2){
						String	broad = str[1];
						if(broad.indexOf("_")==-1){
						String channelName = nce.getName();
		
						ThreeStringWritable cbu = new ThreeStringWritable(channelName,broad,url+"_"+sid);
						if (broad.indexOf("-")!=-1){
							ThreeStringWritable cbu2 = new ThreeStringWritable(channelName,broad.split("-")[0],url+"_"+sid);
							context.write(cbu2, outputValue);
						}
		
					    context.write(cbu, outputValue);
						}
					}
				}
	
			} catch (Exception e) {
				context.getCounter("LogMapper", "mapException").increment(1);
			}
        }
       
    }
    
    public static class SIDTempReducer extends Reducer<ThreeStringWritable, IntWritable, ThreeStringWritable, IntWritable> {
    	
    	
    	private IntWritable outputValue = new IntWritable(1);
    	
        @Override
        protected void reduce(ThreeStringWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       
			context.write(key, outputValue);
        }
    }
    
    public static class SIDMapper extends Mapper<ThreeStringWritable, IntWritable, ThreeStringWritable, IntWritable> {
    	
        @Override
        public void map(ThreeStringWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
         	ThreeStringWritable cbu = new ThreeStringWritable(key.getfirst(),key.getsecond(),key.getthird().split("_")[0]);
        	context.write(cbu, value);
        }
    }
    
    public static class SIDReducer extends Reducer<ThreeStringWritable, IntWritable, ThreeStringWritable, IntWritable> {
    
    	
        @Override
        protected void reduce(ThreeStringWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	 
        	int sum = 0;
        	
        	for(IntWritable val : values){
        	 sum+= val.get();
        	}
           	context.write(key, new IntWritable(sum));
		}
  
    }
    
      
}


