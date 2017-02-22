package com.netease.weblogOffline.statistics.gentiefilter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.Lz4Codec;
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
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.GenTieInfo;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 *  通过跟帖的数据，计算当日每个频道跟帖uv、每个频道下专题跟帖uv,为hive表提供数
 * */
public class GenTieUCPDPUMR extends MRJob {
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.GEN_TIE_INFO + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "GenTieUCPDPUMRTemp/" + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "GenTieUCPDPUMR/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");

    	MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), TextInputFormat.class, GenTieUCPDPUMRMapper.class);
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(GenTieInfo.class);
        
        //reducer
        job1.setReducerClass(GenTieUCPDPUMRReducer.class);
        job1.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(GenTieInfo.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
      	Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step2");

    	MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, CountMapper.class);
        //mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(GenTieInfo.class);
        
        //reducer
        job2.setReducerClass(CountReducer.class);
        job2.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(TextOutputFormat.class);
    
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        
        TextOutputFormat.setCompressOutput(job2, true);  
        TextOutputFormat.setOutputCompressorClass(job2, Lz4Codec.class);
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class GenTieUCPDPUMRMapper extends Mapper<LongWritable, Text, Text, GenTieInfo> {
    	
    	private Text outputKey = new Text();
    	
    	private Map<String, String> map = new HashMap<String, String>();
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
        		map.clear();
	//        	url,pdocid,docid,发帖用户id,跟帖时间，跟帖id
	        	String[] strs = value.toString().split(",");
	        	String url = strs[0];
	        	String pdocid = strs[1];
	        	String docid = strs[2];
	        	String uid = strs[3];
	        	
	        	String channel = NeteaseChannel_CS.getChannelName(url);
	        	
	        	
	        	map.put("url", url);
	        	map.put("uid", uid);
	        	GenTieInfo gti = new GenTieInfo(pdocid,docid,channel,1);
	        	
	        	
	        	outputKey.set(JsonUtils.toJson(map));
	        	context.write(outputKey, gti);
			} catch (Exception e) {
				context.getCounter("GenTieUCPDPUMRMapper", "mapException").increment(1);
			}
        }
    }
    
	public static class GenTieUCPDPUMRReducer extends Reducer<Text, GenTieInfo, Text, GenTieInfo> {
		
		private Text outputKey = new Text();
		
        @Override
        protected void reduce(Text key, Iterable<GenTieInfo> values, Context context) throws IOException, InterruptedException {
        	Map<String, String> tempMap = JsonUtils.json2Map(key.toString());
        	outputKey.set(tempMap.get("url"));
        	GenTieInfo gti = new GenTieInfo();
        	int i= 0;
        	for(GenTieInfo val : values){
        		if (i==0){
        			gti.setPdocid(val.getPdocid());
        			gti.setDocid(val.getDocid());
        			gti.setChannel(val.getChannel());
        			gti.setCount(gti.getCount()+val.getCount());
        		
        		}else{
        			gti.setCount(gti.getCount()+val.getCount());
        		}
        		i++;
        	}
        	       	
        	context.write(outputKey, gti);
        }
	}
	
    public static class CountMapper extends Mapper<Text, GenTieInfo, Text, GenTieInfo> {
        @Override
        public void map(Text key, GenTieInfo value, Context context) throws IOException, InterruptedException {
        	context.write(key, value);
        }
    }
    
	public static class CountReducer extends Reducer<Text, GenTieInfo, Text, NullWritable> {
		
		private Text outputKey = new Text();
		
        @Override
        protected void reduce(Text key, Iterable<GenTieInfo> values, Context context) throws IOException, InterruptedException {
        	int uv = 0;
        	int pv = 0;
        	int i=0;
        	GenTieInfo gti = new GenTieInfo();
        	for(GenTieInfo val : values){
        		gti = val;
        		uv++;
        		pv += val.getCount() ;
        	}
        	String s = key.toString()+","+gti.getPdocid()+","+gti.getDocid()+","+gti.getChannel()+","+pv + "," + uv;
        	outputKey.set(s.replace("\n", " ").replace("\t", " "));
        	context.write(outputKey, NullWritable.get());
        }
	}
}












