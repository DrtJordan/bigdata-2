package com.netease.weblogOffline.statistics.dailyWeblog;

import java.io.IOException;
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
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogCommon.utils.TopNTool;
import com.netease.weblogCommon.utils.TopNTool.SortElement;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.LongStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

public class ShareBackTopNMR extends MRJob {
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "shareBackCount/" + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_OTHER_DIR + "shareBackTopN/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());
        	
        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, SBMapper.class);
        
        
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongStringWritable.class);
        
        //reducer
        job.setReducerClass(CounterReducer.class);
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
    
    public static class SBMapper extends Mapper<Text, Text, Text, LongStringWritable> {
    	
    	private Text outputkey = new Text();
    	private LongStringWritable outputValue = new LongStringWritable();
    	
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	Map<String, String> map = JsonUtils.json2Map(value.toString());
        	String keyPrefix = map.get("channel") + "_" + map.get("type") + "_" ;
        	for(Entry<String, String> entry : map.entrySet()){
        		try {
					if(entry.getKey().startsWith("s_") || entry.getKey().startsWith("b_")){
						outputkey.set(keyPrefix + entry.getKey()); 

						int count = Integer.parseInt(entry.getValue());
						String url = key.toString();
						outputValue.setFirst(count);
						outputValue.setSecond(url);
						
						context.write(outputkey, outputValue);
						
						outputkey.set("all_all_" + entry.getKey().split("_")[0] + "_all");
						context.write(outputkey, outputValue);
					}
				} catch (Exception e) {
					context.getCounter("SBMapper", "mapError").increment(1);
					
				}
        	}
        }
    }
    
    public static class CounterReducer extends Reducer<Text, LongStringWritable, Text, Text> {
    	
    	private static final int n = 10;
    	
    	private Text outputValue = new Text();
    	
        @Override
        protected void reduce(Text key, Iterable<LongStringWritable> values, Context context) throws IOException, InterruptedException {

        	try {
				TopNTool topNTool = new TopNTool(n);
				
				for(LongStringWritable value : values){
					topNTool.addElement(new SortElement(value.getFirst(), value.getSecond()));
				}
				
				for(SortElement ele : topNTool.getTopN()){
					long count = ele.getCount();
					String url = ele.getVal().toString();
					outputValue.set(count + "\t" + url);
					context.write(key, outputValue);
				}
			} catch (Exception e) {
				context.getCounter("CounterReducer", "exceptions").increment(1);
			}
        }
    }
}












