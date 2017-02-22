package com.netease.weblogOffline.statistics.contentscore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogCommon.data.enums.ShareBackChannel_CS;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.logparsers.ZyLogParams;
import com.netease.weblogCommon.logparsers.ZylogParser;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 从章鱼日志中计算每个url当日在各个渠道的分享、回流次数
 * */
public class ShareBackCountMR extends MRJob {
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.ZY_LOG + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "shareBackCount/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());

    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), TextInputFormat.class, LogMapper.class);
        
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        //reducer
        job.setReducerClass(CountReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
    	
    	private Text outputkey = new Text();
    	private Text outputValue = new Text();
    	
    	private LogParser logParser = new ZylogParser(); 
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	String oneLog = value.toString();
        	try {
				Map<String, String> logMap = logParser.parse(oneLog);
				String url = logMap.get(ZyLogParams.url);
				String prefix = null;
				String suffix = null;
				
				if(null != (suffix = ShareBackChannel_CS.getShareChannel(url))){
					prefix = "s";
				}else if(null != (suffix = ShareBackChannel_CS.getBackChannel(url))){
					prefix = "b";
				}else{
					return;
				}
				
				String pureUrl = UrlUtils.getOriginalUrl(url);
				outputkey.set(pureUrl);
				
				Map<String, Integer> map = new HashMap<String, Integer>();
				String k = prefix + "_" + suffix;
				map.put(k, 1);
				outputValue.set(JsonUtils.toJson(map));
				
				context.write(outputkey, outputValue);
			} catch (Exception e) {
				context.getCounter("LogMapper", "mapException").increment(1);
			}
        }
    }
    
    public static class CountReducer extends Reducer<Text, Text, Text, Text> {
    	
    	private Text outputValue = new Text();
    	
    	private Map<String, Integer> counterMap = new HashMap<String, Integer>();
    	private Map<String, String> outputMap = new TreeMap<String, String>();
    	
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	counterMap.clear();
        	outputMap.clear();
        	
			for (Text value : values) {
				Map<String, Object> map = JsonUtils.json2ObjMap(value.toString());
				for(Entry<String, Object> e : map.entrySet()){
					Integer oldV = counterMap.get(e.getKey());
					if(null == oldV){
						oldV = 0;
					}
					counterMap.put(e.getKey(), oldV + (Integer) e.getValue());
				}
			}
			
			for(Entry<String, Integer> e : counterMap.entrySet()){
				outputMap.put(e.getKey(), String.valueOf(e.getValue()));
			}
			
			String url = key.toString();
			String channel = NeteaseChannel_CS.getChannelName(url);
			String type = NeteaseContentType.getTypeName(url); 
			
			outputMap.put("channel", channel);
			outputMap.put("type", type);
			
			outputValue.set(JsonUtils.toJson(outputMap));
			context.write(key, outputValue);
        }
    }
}












