package com.netease.weblogOffline.statistics.gentieInfo;

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

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogCommon.utils.SimpleTopNTool;
import com.netease.weblogCommon.utils.SimpleTopNTool.SortElement;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.LongStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 *  
 * */
public class GenTieChannelTopurlMR extends MRJob {
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.GEN_TIE_INFO + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_DIR+"result_other/GenTieChannelTopurlCount/" + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_DIR+"result_other/GenTieChannelTopurlTopN/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() +"step1");

    	MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), TextInputFormat.class, GenTieInfoMapper.class);
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job1.setReducerClass(DistinctReducer.class);
        job1.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
    	Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() +"step1");

    	MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, GenTieUrlTopNMapper.class);
        //mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongStringWritable.class);
        
        //reducer
        job2.setReducerClass(GenTieUrlTopNReducer.class);
        job2.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class GenTieInfoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    	
    	private Text outputKey = new Text();
    	private IntWritable outputValue = new IntWritable(1);
    	private Map<String, String> map = new HashMap<String, String>();
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
         		map.clear();
    			// url,pdocid,docid,发帖用户id,跟帖时间，跟帖id,ip,source
				String[] strs = value.toString().split(",");
				String url = strs[0];
			String source = strs[7];
				String channel = NeteaseChannel_CS.getChannelName(url);
				
				//NeteaseChannel  Nchannel = NeteaseChannel.getChannel(url);
				//if(Nchannel!=null){
				//	String channel = Nchannel.getName();
		        	
		        	map.put("channel", channel);
		        	map.put("url", url);
		        	
		        	outputKey.set(JsonUtils.toJson(map));

		        	context.write(outputKey,outputValue);
				//}
	        	
			} catch (Exception e) {
				context.getCounter("GenTieInfoMapper", "mapException").increment(1);
			}
        }
    }
    
	public static class DistinctReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable outputValue = new IntWritable();
		
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable i :values){
        	sum +=i.get();
        }
        outputValue.set(sum);
        context.write(key, outputValue);
        }
	}
	
	
	 public static class GenTieUrlTopNMapper extends Mapper<Text, IntWritable, Text, LongStringWritable> {
	    	
	    	Text outptKey = new Text();
	        @Override
	        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
	        	try {

	        	  	Map<String, String> keymap = JsonUtils.json2Map(key.toString());
		            String channel=keymap.get("channel");
		        	
		        	LongStringWritable ls = new LongStringWritable((long)value.get(),keymap.get("url"));
		        	outptKey.set(channel);
		        	context.write(outptKey,ls);
				} catch (Exception e) {
					context.getCounter("GenTieInfoMapper", "mapException").increment(1);
				}
	        }
	    }
	    
		public static class GenTieUrlTopNReducer extends Reducer<Text, LongStringWritable, Text, IntWritable> {
	    	
			private Text outputKey = new Text();
			private IntWritable outputValue = new IntWritable();
			SimpleTopNTool utl = new SimpleTopNTool(10);
	        @Override
	        protected void reduce(Text key, Iterable<LongStringWritable> values, Context context) throws IOException, InterruptedException {
	       
	        for(LongStringWritable ls :values){
	        	utl.addElement(new SortElement(ls.getFirst(), ls.getSecond()));
	        }
	        
	        for(SortElement ele : utl.getTopN()){
	        	
	        	outputKey.set(key.toString()+"\t"+ele.getVal().toString());
	        	outputValue.set((int)ele.getCount());
	            context.write(outputKey, outputValue);
			}
	        }
		}

}












