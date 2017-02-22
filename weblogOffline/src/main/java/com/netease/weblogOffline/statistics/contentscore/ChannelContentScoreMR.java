package com.netease.weblogOffline.statistics.contentscore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogCommon.utils.TextUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.PathFilters;
import com.netease.weblogOffline.data.ContentScoreVector;
import com.netease.weblogOffline.utils.HadoopUtils;

public class ChannelContentScoreMR extends MRJob {
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.WEBLOG_MIDLAYER_DIR + "contentScoreVector/" + date);
		inputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "genTieChannelSpecialUv/" + date);
		inputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "genTieYcUv/" + date);
		inputList.add(DirConstant.WEBLOG_STATISTICS_OTHER_DIR + "ChannelHomeFromWeblog/" + date);
		inputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR +"ChannelAndHomePvUv/" + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_OTHER_DIR + "channelContentScoreTemp/" + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_OTHER_DIR + "channelContentScore/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");

    	MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, ContentScoreVectorMapper.class);
    	
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(ContentScoreVector.class);
        
        //reducer
        job1.setReducerClass(CountReducer.class);
        job1.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
     	Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step2");
     	
    	Configuration conf =job2.getConfiguration();
        conf.set("channelandhomepvuv",inputList.get(4)+"/");

    	MultipleInputs.addInputPath(job2, new Path(inputList.get(1)), SequenceFileInputFormat.class, MergeMapper.class);
    	MultipleInputs.addInputPath(job2, new Path(inputList.get(2)), SequenceFileInputFormat.class, MergeMapper.class);
    	MultipleInputs.addInputPath(job2, new Path(inputList.get(3)), TextInputFormat.class, ChannelHomeMapper.class);
    	MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, MergeMapper.class);
    	
        //mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        
        //reducer
        job2.setReducerClass(MargeReducer.class);
        job2.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class ContentScoreVectorMapper extends Mapper<Text, ContentScoreVector, Text, ContentScoreVector> {
    	
    	private Text outputKey = new Text();
    	
        @Override
        public void map(Text key, ContentScoreVector value, Context context) throws IOException, InterruptedException {
        	outputKey.set(value.getChannel());
        	context.write(outputKey, value);
        	
        	if(value.isYc()){
        		outputKey.set(value.getChannel() + "_yc" );
            	context.write(outputKey, value);
        	}
        	
        	if(NeteaseContentType.special.match(value.getUrl())
        			&& !value.isYc()){
        		outputKey.set(value.getChannel() + "_special" );
            	context.write(outputKey, value);
        	}
        }
    }
    
    public static class CountReducer extends Reducer<Text, ContentScoreVector, Text, Text> {
    	
    	private Text outputValue = new Text();
    	
      	private Map<String, String> map = new HashMap<String, String>();
    	
        @Override
        protected void reduce(Text key, Iterable<ContentScoreVector> values, Context context) throws IOException, InterruptedException {
        	map.clear();
        	
        	String k = key.toString();
        	String channel = k.split("_")[0];
        	int ycCount = 0;
        	int genTieCountSum = 0;
        	int shareCountSum = 0;
        	int backCountSum = 0;
        	int pvSum = 0;
        	
        	for(ContentScoreVector csv : values){
        		
        		if(csv.isYc()){
        			ycCount++;
        		}
        		genTieCountSum += csv.getGenTieCount();
        		shareCountSum += csv.getShareCount();
        		backCountSum += csv.getBackCount();
        		pvSum += csv.getPv();
    		}
        	
        	map.put("channel", channel);
        	map.put("pvSum", String.valueOf(pvSum));
        	map.put("genTieCountSum", String.valueOf(genTieCountSum));
        	map.put("shareCountSum", String.valueOf(shareCountSum));
        	map.put("backCountSum", String.valueOf(backCountSum));
        	map.put("ycCount", String.valueOf(ycCount));
        	
        	outputValue.set(JsonUtils.toJson(map));
        	context.write(key, outputValue);
        }
    }
    
    public static class MergeMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	context.write(key, value);
        }
    }
    
    public static class ChannelHomeMapper extends Mapper<LongWritable, Text, Text, Text> {
    	
    	private Text outputKey = new Text();
    	private Text outputValue = new Text();
    	
      	private Map<String, String> map = new HashMap<String, String>();
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	map.clear();
        	String v = value.toString();
        	String[] strs = v.split("\t");
        	if(strs[0].startsWith("special")){
        		outputKey.set(strs[0].substring(7, strs[0].length()) + "_special");
        		map.put("avgclick", strs[8]);
        		outputValue.set(JsonUtils.toJson(map));
        		context.write(outputKey, outputValue);
        	}else if(strs[0].startsWith("home")){
        		outputKey.set(strs[0].substring(4, strs[0].length()) + "_channelHome");
        		map.put("avgclick", strs[8]);
        		map.put("avgvisit", strs[9]);
        		map.put("avgclickvisit", strs[10]);
        		outputValue.set(JsonUtils.toJson(map));
        		context.write(outputKey, outputValue);
        	}else if(strs[0].startsWith("all")){
        		outputKey.set(strs[0].substring(3, strs[0].length()) + "_channelAll");
        		map.put("avgclick", strs[8]);
        		map.put("avgvisit", strs[9]);
        		map.put("avgclickvisit", strs[10]);
        		outputValue.set(JsonUtils.toJson(map));
        		context.write(outputKey, outputValue);
        	}
        }
    }
    
    public static class MargeReducer extends Reducer<Text, Text, Text, NullWritable> {
    	private Text outputKey = new Text();
    	
    	private Map<String, Map<String,String>> outputMap = new HashMap<String, Map<String,String>>();
       	private static HashMap<String,String>  channelAndHomeInfo;
      	
      	private final String intDef = "0" ;
      	
      	private final String channelInfluenceDefRes = "0\t0\t0\t0\t0";
      	private final String ycInfluenceDefRes = "0\t0\t0\t0\t0";
      	private final String specialInfluenceDefRes = "0\t0\t0\t0\t0\t0";
      	private final String channelHomeDefRes = "0\t0\t0";
      	private final String channelAllDefRes = "0\t0\t0";
      	private final String channelandhomePvUvDef = "0\t0\t0\t0";
      	

    	
        @Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
         	Configuration conf = context.getConfiguration();
        	String channelandhomepvuv = conf.get("channelandhomepvuv");
        	channelAndHomeInfo = loadSeqFileToMap(context,channelandhomepvuv);
		}
		private HashMap<String,String> loadSeqFileToMap(Context context,String channelandhomepvuv) {
			HashMap<String,String> hm = new HashMap<String,String>();
			
			SequenceFile.Reader hdfsReader = null;
	    	Text key = new Text();
	    	Text value = new Text();

			try {
				FileSystem	hdfs = FileSystem.get(context.getConfiguration());
				for(FileStatus status : hdfs.listStatus(new Path(channelandhomepvuv),PathFilters.partFilter())){
					        hdfsReader = new SequenceFile.Reader(hdfs, status.getPath(), context.getConfiguration());
					    	while (hdfsReader.next(key, value)) {
					    		hm.put(key.toString(), value.toString());
					}
				}	
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				if (hdfsReader!=null ){
					hdfsReader.close();
				}
		
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return hm;
		}


		@Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	Map<String, String> map = new HashMap<String, String>();
        	
        	String k = key.toString();
        	String channel = k.split("_")[0];
        	
        	for(Text val : values){
        		try {
					Map<String, String> tempMap = JsonUtils.json2Map(val.toString());
					map.putAll(tempMap);
				} catch (Exception e) {
					context.getCounter("MargeReducer", "reducerException").increment(1);
				}
        	}
        	
        	String baseOutputPath = null;
         	String res = null;
         	
         	if(k.contains("_yc")){
        		baseOutputPath = "ycInfluence";
        		res =  TextUtils.notNullStr(map.get("pvSum"), intDef) + "\t" + TextUtils.notNullStr(map.get("genTieCountSum"), intDef) + "\t"
        				+ TextUtils.notNullStr(map.get("genTieUvSum"), intDef) + "\t" + TextUtils.notNullStr(map.get("shareCountSum"), intDef) + "\t"
        				+ TextUtils.notNullStr(map.get("backCountSum"), intDef);
        	} else if(k.contains("_special")){
        		baseOutputPath = "specialInfluence";
        		res = TextUtils.notNullStr(map.get("avgclick"), intDef) + "\t" + TextUtils.notNullStr(map.get("pvSum"), intDef) + "\t"
        				+ TextUtils.notNullStr(map.get("genTieCountSum"), intDef) + "\t" + TextUtils.notNullStr(map.get("genTieUvSum"), intDef) + "\t"
        				+ TextUtils.notNullStr(map.get("shareCountSum"), intDef) + "\t" + TextUtils.notNullStr(map.get("backCountSum"), intDef);
        	}else if(k.contains("_channelHome")){
        		baseOutputPath = "channelHome";
        		res =  TextUtils.notNullStr(map.get("avgclick"), intDef)  + "\t" + TextUtils.notNullStr(map.get("avgvisit"), intDef)  + "\t" 
        				+ TextUtils.notNullStr(map.get("avgclickvisit"), intDef);
        	}else if(k.contains("_channelAll")){
        		baseOutputPath = "channelAll";
        		res =  TextUtils.notNullStr(map.get("avgclick"), intDef)  + "\t" + TextUtils.notNullStr(map.get("avgvisit"), intDef)  + "\t" 
        				+ TextUtils.notNullStr(map.get("avgclickvisit"), intDef);
        	}else{
        		baseOutputPath = "channelInfluence";
        		res =  TextUtils.notNullStr(map.get("ycCount"), intDef)  + "\t" + TextUtils.notNullStr(map.get("genTieCountSum"), intDef)  + "\t" 
        				+ TextUtils.notNullStr(map.get("genTieUvSum"), intDef)  + "\t" + TextUtils.notNullStr(map.get("shareCountSum"), intDef)  + "\t" 
        				+ TextUtils.notNullStr(map.get("backCountSum"), intDef) ;
        	}
        	
         	Map<String, String> subMap = outputMap.get(channel);
         	if(null == subMap){
         		subMap = new HashMap<String, String>();
         		outputMap.put(channel, subMap);
         	}
         	subMap.put(baseOutputPath, res);
        }
        
        
      	
    	@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
    		for(String channel : NeteaseChannel_CS.getVaildChannelNameForScore()){
    			String channelInfluenceRes = channelInfluenceDefRes;
    			String ycInfluenceRes = ycInfluenceDefRes;
    			String specialInfluenceRes = specialInfluenceDefRes;
    			String channelAllRes = channelAllDefRes;
    			String channelHomeRes = channelHomeDefRes;
    			String channelandhomePvUv =channelandhomePvUvDef;
    			
    			channelandhomePvUv = channelAndHomeInfo.get(channel);
    			
    			Map<String, String> tempMap = outputMap.get(channel);
    			if(null != tempMap){
    				if(StringUtils.isNotBlank(tempMap.get("channelInfluence"))){
    					channelInfluenceRes = tempMap.get("channelInfluence");
    				}
    				if(StringUtils.isNotBlank(tempMap.get("ycInfluence"))){
    					ycInfluenceRes = tempMap.get("ycInfluence");
    				}
    				if(StringUtils.isNotBlank(tempMap.get("specialInfluence"))){
    					specialInfluenceRes = tempMap.get("specialInfluence");
    				}
    				if(StringUtils.isNotBlank(tempMap.get("channelAll"))){
    					channelAllRes = tempMap.get("channelAll");
    				}
    				if(StringUtils.isNotBlank(tempMap.get("channelHome"))){
    					channelHomeRes = tempMap.get("channelHome");
    				}
    			}
    			
    			outputKey.set(channel + "\t" + channelInfluenceRes + "\t" + ycInfluenceRes + "\t" + specialInfluenceRes + "\t" + channelAllRes + "\t" + channelHomeRes+"\t"+channelandhomePvUv);
    			context.write(outputKey, NullWritable.get());
    		}
		}
        
    }
}












