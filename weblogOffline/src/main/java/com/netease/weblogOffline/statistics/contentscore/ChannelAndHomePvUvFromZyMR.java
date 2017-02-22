package com.netease.weblogOffline.statistics.contentscore;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.slf4j.LoggerFactory;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.logparsers.ZyLogParams;
import com.netease.weblogCommon.logparsers.ZylogParser;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.utils.HadoopUtils;


/**
 * ChannelAndHomePvUv
 *
 */

public class ChannelAndHomePvUvFromZyMR extends MRJob {
	
	private static final org.slf4j.Logger log = LoggerFactory.getLogger(ChannelAndHomePvUvFromZyMR.class);

	@Override
	public boolean init(String date) {
        //输入列表
        inputList.add(DirConstant.ZY_LOG + date);
        
    	outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "ChannelAndHomePvUvTemp/" + date);
        //输出列表
        outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR +"ChannelAndHomePvUv/" + date);
 

		return true;
	}

    
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");


    	MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), TextInputFormat.class, LogMapper.class);
        
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        
        job1.setCombinerClass(PvUvTempReducer.class);
        
        //reducer
        job1.setReducerClass(PvUvTempReducer.class);
        job1.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
    	Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step2");

    	MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, PvUvMapper.class);
        
        //mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job2.setReducerClass(PvUvReducer.class);
        job2.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    	
    	private LogParser logParser = new ZylogParser(); 
    	
    	private Text outputkey = new Text();
    	private IntWritable outputValue = new IntWritable(1);
    	private Map<String, String>  channelmap = new HashMap<String, String>();
    	private Map<String, String>  homemap = new HashMap<String, String>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	channelmap.clear();
        	homemap.clear();
        	
        	String oneLog = value.toString();
        	try {
				Map<String, String> logMap = logParser.parse(oneLog);
				String url = logMap.get(ZyLogParams.url);
				
				NeteaseChannel_CS nce = NeteaseChannel_CS.getChannel(url);
				if(nce != null){
					String uid = logMap.get(ZyLogParams.uid);
					String channelName = nce.getName();
					if(nce.isHome(url)){
						homemap.put("CHANNELorHOME", channelName+"_HOME");
						homemap.put("uid", logMap.get(ZyLogParams.uid));
						channelmap.put("CHANNELorHOME", channelName);
						channelmap.put("uid", uid);
						outputkey.set(JsonUtils.toJson(homemap));
						context.write(outputkey, outputValue);
						outputkey.set(JsonUtils.toJson(channelmap));
						context.write(outputkey, outputValue);
					}else {
						channelmap.put("CHANNELorHOME", channelName);
						channelmap.put("uid", uid);
						outputkey.set(JsonUtils.toJson(channelmap));
						context.write(outputkey, outputValue);
					}
				}
	
			} catch (Exception e) {
				context.getCounter("LogMapper", "mapException").increment(1);
			}
        }
       
    }
    
    public static class PvUvTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
    	private IntWritable outputValue = new IntWritable();
    	
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum = 0;
        	
        	for(IntWritable val : values){
        		sum += val.get();
        	}
        	
        	outputValue.set(sum);
			context.write(key, outputValue);
        }
    }
    
    public static class PvUvMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
    	private Text outputKey = new Text();
    	
        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        	Map<String,String> map = JsonUtils.json2Map(key.toString());
        	outputKey.set(map.get("CHANNELorHOME"));
        	context.write(outputKey, value);
        }
    }
    
    public static class PvUvReducer extends Reducer<Text, IntWritable, Text, Text> {
    	

    	private HashMap<String,HashMap<String,Integer>> channelmap = new HashMap<String, HashMap<String,Integer>>();
    	private final String intDef = "0" ;
    	
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	
        	int pv = 0;
        	int uv = 0;
        	
        	for(IntWritable val : values){
        		pv += val.get();
        		uv++;
        	}
        	
        	
            String keyString = key.toString();
        	if (keyString.indexOf("_HOME")!=-1){
        		if (channelmap.get(keyString.split("_")[0])!=null){
        			channelmap.get(keyString.split("_")[0]).put("homepv", pv);
        			channelmap.get(keyString.split("_")[0]).put("homeuv", uv);
        		}else{
        			HashMap<String,Integer> pumap = new HashMap<String, Integer>();
            		pumap.put("homepv", pv);
                	pumap.put("homeuv", uv);
                 	channelmap.put(keyString.split("_")[0], pumap);
        		}
        
        	}else{
         		if (channelmap.get(keyString)!=null){
        			channelmap.get(keyString).put("pv", pv);
        			channelmap.get(keyString).put("uv", uv);
        		}else{
        			HashMap<String,Integer> pumap = new HashMap<String, Integer>();
            		pumap.put("pv", pv);
                	pumap.put("uv", uv);
                 	channelmap.put(keyString, pumap);
        		}
        	}
       

        }


		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		    for(String channelname:channelmap.keySet()){
		    	HashMap<String,Integer> tempmap = channelmap.get(channelname);
	    		StringBuilder sb = new StringBuilder();
		    	if (tempmap.get("pv")!=null){
			    	sb.append(tempmap.get("pv")).append("\t");
		    	}else {
		    		sb.append(intDef).append("\t");
		    	}
		    	if (tempmap.get("uv")!=null){
			    	sb.append(tempmap.get("uv")).append("\t");
		    	}else {
		    		sb.append(intDef).append("\t");
		    	}	
		    	if (tempmap.get("homepv")!=null){
			    	sb.append(tempmap.get("homepv")).append("\t");
		    	}else {
		    		sb.append(intDef).append("\t");
		    	}	
		    	if (tempmap.get("homeuv")!=null){
			    	sb.append(tempmap.get("homeuv"));
		    	}else {
		    		sb.append(intDef);
		    	}	
		    	context.write(new Text(channelname), new Text(sb.toString()));
		    }
		}
  
    }
    	  
	  
}


