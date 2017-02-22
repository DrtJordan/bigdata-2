package com.netease.weblogOffline.statistics.changechannel;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.weblogfilter.WeblogFilterUtils;
import com.netease.weblogOffline.utils.HadoopUtils;


/**
 * Channel  launch事件，email为空，uuid去重
 *          launch事件，email,uuid去重
 */

public class EmailOrNoEmailUvFromWeblogMR extends MRJob {
	
	private static final org.slf4j.Logger log = LoggerFactory.getLogger(EmailOrNoEmailUvFromWeblogMR.class);

	@Override
	public boolean init(String date) {
        //输入列表
      //  inputList.add(DirConstant.ZY_LOG + date);
		
		inputList.add(DirConstant.WEBLOG_FilterLOG
				+ date);
        
    	outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "EmailOrNoEmailUvTemp/" + date);
        //输出列表
        outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "EmailOrNoEmailUvUv/" + date);
 

		return true;
	}

    
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");


    	MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, LogMapper.class);
        
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
    
    public static class LogMapper extends Mapper<NullWritable, Text, Text, IntWritable> {
    	

    	
    	private Text outputkey = new Text();
    	private IntWritable outputValue = new IntWritable(1);
    	private Map<String, String>  channelmap = new HashMap<String, String>();
       	private Map<String, String>  channeltimemap = new HashMap<String, String>();
        @Override
        public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        	channelmap.clear();
        	channeltimemap.clear();

        	try {
        		HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());
 
        		String url =lineMap.get("url");
        		String event = lineMap.get("event");
				
				NeteaseChannel_CS nce = NeteaseChannel_CS.getChannel(url);
				String email = lineMap.get("cvar_email");
				
				if(event.equals("launch")&&nce != null&&NeteaseContentType.artical.match(url)){
					String str[] = lineMap.get("project").split("@version@");

					if (str.length==2){
						String broad = str[1];
					if(broad.indexOf("_")==-1){
				    String uuid =lineMap.get("uuid");
					String channelName = nce.getName();
					String[] broadtime =broad.split("-");
					if (!email.equals("(null)")){
				
						if (broadtime.length==2){
							channeltimemap.put("ChannelEmailOfBroad", channelName+"_"+broadtime[0]+"_EMAIL");
							channeltimemap.put("uid", uuid);
							outputkey.set(JsonUtils.toJson(channeltimemap));
							context.write(outputkey, outputValue);
						}
						
						channelmap.put("ChannelEmailOfBroad", channelName+"_"+broad+"_EMAIL");
						channelmap.put("uuid", uuid);
						outputkey.set(JsonUtils.toJson(channelmap));
						context.write(outputkey, outputValue);
					}else{
						
						if (broadtime.length==2){
							channeltimemap.put("ChannelEmailOfBroad", channelName+"_"+broadtime[0]+"_NOEMAIL");
							channeltimemap.put("uid", uuid);
							outputkey.set(JsonUtils.toJson(channeltimemap));
							context.write(outputkey, outputValue);
						}
						channelmap.put("ChannelEmailOfBroad", channelName+"_"+broad+"_NOEMAIL");
						channelmap.put("uuid", uuid);
						outputkey.set(JsonUtils.toJson(channelmap));
						context.write(outputkey, outputValue);
					    }
						}
					}
		
				}
	
			} catch (Exception e) {
				context.getCounter("LogMapper", "mapException").increment(1);
			}
        }
       
    }
    
    public static class PvUvTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
    	private IntWritable outputValue = new IntWritable(1);
    	
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
     
			context.write(key, outputValue);
        }
    }
    
    public static class PvUvMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
    	private Text outputKey = new Text();
    	
        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        	Map<String,String> map = JsonUtils.json2Map(key.toString());
        	outputKey.set(map.get("ChannelEmailOfBroad"));
        	context.write(outputKey, value);
        }
    }
    
    public static class PvUvReducer extends Reducer<Text, IntWritable, Text, Text> {
    	

    	private HashMap<String,Integer> channelmap = new HashMap<String,Integer>();
    	
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	
        	int uv = 0;
        	
        	for(IntWritable val : values){
        		uv++;
        	}
        	
        	
            String keyString = key.toString();

	
         	channelmap.put(keyString, uv);

        }


		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		    for(String channelemail:channelmap.keySet()){
	
		    	 
		    	context.write(new Text(channelemail), new Text(channelmap.get(channelemail).toString()));

		    }
		    
    	
		  
		}
  
    }
    	  
	  
}


