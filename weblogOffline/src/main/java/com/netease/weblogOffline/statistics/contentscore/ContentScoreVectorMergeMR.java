package com.netease.weblogOffline.statistics.contentscore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.ContentScoreVector;
import com.netease.weblogOffline.data.LongLongWritable;
import com.netease.weblogOffline.data.YcInfo;
import com.netease.weblogOffline.utils.HadoopUtils;

public class ContentScoreVectorMergeMR extends MRJob {
	@Override
	public boolean init(String date) {
		//ShareBack
		inputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "shareBackCount/" + date);
		inputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "contentPvUv/" + date);
		inputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "genTieCount/" + date);
		inputList.add(DirConstant.YC_INFO_ALL + date);
		
		outputList.add(DirConstant.WEBLOG_MIDLAYER_DIR + "contentScoreVector/" + date);
		
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());

    	//分享、回流
    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, ShareBackMapper.class);
    	//pv、uv
    	MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, ContentPvUvMapper.class);
    	//跟帖
    	MultipleInputs.addInputPath(job, new Path(inputList.get(2)), SequenceFileInputFormat.class, GenTieMapper.class);
    	//cms数据
    	MultipleInputs.addInputPath(job, new Path(inputList.get(3)), SequenceFileInputFormat.class, CmsYcInfoMapper.class);
    	
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        //reducer
        job.setReducerClass(MergeReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ContentScoreVector.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class ShareBackMapper extends Mapper<Text, Text, Text, Text> {
    	
    	private Text outputValue = new Text();
    	private Map<String,Integer> outputMap = new HashMap<String, Integer>();
    	
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	outputMap.clear();
        	Map<String, String> map = JsonUtils.json2Map(value.toString());
        	int shareCount = 0;
        	int backCount = 0;
        	
        	for(Entry<String, String> entry : map.entrySet()){
        		if(entry.getKey().startsWith("s_")){
        			int count = Integer.parseInt(entry.getValue());
        			shareCount += count;
        		}else if(entry.getKey().startsWith("b_")){
        			int count = Integer.parseInt(entry.getValue());
        			backCount += count;
        		}
        	}
        	
        	outputMap.put("shareCount", shareCount);
        	outputMap.put("backCount", backCount);
        	outputValue.set(JsonUtils.toJson(outputMap));
        	
        	context.write(key, outputValue);
        }
    }
    
    public static class ContentPvUvMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	context.write(key, value);
        }
    }
    
    public static class CmsYcInfoMapper extends Mapper<Text, YcInfo, Text, Text> {
    	private Text outputValue = new Text();
    	private Map<String,String> outputMap = new HashMap<String, String>();
    	
        @Override
        public void map(Text key, YcInfo value, Context context) throws IOException, InterruptedException {
        	outputMap.clear();
        	outputMap.put("source", "yc");
        	outputMap.put("lmodify", value.getLmodify());
        	outputMap.put("title", value.getTitle());
        	outputMap.put("author", value.getAuthor());
        	outputValue.set(JsonUtils.toJson(outputMap));
        	context.write(key, outputValue);
        }
    }
    
    public static class GenTieMapper extends Mapper<Text, LongLongWritable, Text, Text> {
    	private Text outputValue = new Text();
    	private Map<String,Integer> outputMap = new HashMap<String, Integer>();
    	
        @Override
        public void map(Text key, LongLongWritable value, Context context) throws IOException, InterruptedException {
        	outputMap.clear();
        	outputMap.put("genTieCount", (int)value.getFirst());
        	outputMap.put("genTieUv", (int)value.getSecond());
        	outputValue.set(JsonUtils.toJson(outputMap));
        	context.write(key, outputValue);
        }
    }
    
    
    public static class MergeReducer extends Reducer<Text, Text, Text, ContentScoreVector> {
    	
    	private Map<String, Object> csvMap = new HashMap<String, Object>(); 
    	
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	csvMap.clear();
        	for(Text val : values){
        		Map<String, Object> tempMap = JsonUtils.json2ObjMap(val.toString());
        		csvMap.putAll(tempMap);
        	}
        	
        	ContentScoreVector csv = new ContentScoreVector();
        	String url = key.toString();
        	
        	csv.setUrl(url);
        	csv.setChannel(NeteaseChannel_CS.getChannelName(url));
        	csv.setType(NeteaseContentType.getTypeName(url));
        	
        	if(csvMap.containsKey("shareCount")){
        		csv.setShareCount((Integer)csvMap.get("shareCount"));
        	}
        	if(csvMap.containsKey("backCount")){
        		csv.setBackCount((Integer)csvMap.get("backCount"));
        	}
        	if(csvMap.containsKey("pv")){
        		csv.setPv((Integer)csvMap.get("pv"));
        	}
        	if(csvMap.containsKey("uv")){
        		csv.setUv((Integer)csvMap.get("uv"));
        	}
        	if(csvMap.containsKey("genTieCount")){
        		csv.setGenTieCount((Integer)csvMap.get("genTieCount"));
        	}
        	if(csvMap.containsKey("genTieUv")){
        		csv.setGenTieUv((Integer)csvMap.get("genTieUv"));
        	}
        	if(csvMap.containsKey("source")){
        		csv.setSource((String)csvMap.get("source"));
        	}
        	if(csvMap.containsKey("lmodify")){
        		csv.setLmodify((String)csvMap.get("lmodify"));
        	}
        	if(csvMap.containsKey("title")){
        		csv.setTitle((String)csvMap.get("title"));
        	}
        	if(csvMap.containsKey("author")){
        		csv.setAuthor((String)csvMap.get("author"));
        	}
        	
        	context.write(key, csv);
        }
    }
}













