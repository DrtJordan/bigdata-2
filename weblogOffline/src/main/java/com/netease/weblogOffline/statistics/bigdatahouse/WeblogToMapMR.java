package com.netease.weblogOffline.statistics.bigdatahouse;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.weblogfilter.WeblogFilterUtils;
import com.netease.weblogOffline.data.HashMapStringStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;


/**
 *  从大数据到统一的map结果
 * */
public class WeblogToMapMR extends MRJob {
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.WEBLOG_FilterLOG + date);
		outputList.add(DirConstant.HOUSE_WEBLOG_MAP+ date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());

    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, WeblogToHashMapMapper.class);
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(HashMapStringStringWritable.class);
        
        //reducer
        job.setReducerClass(WeblogToHashMapReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(HashMapStringStringWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class WeblogToHashMapMapper extends Mapper<NullWritable, Text, Text, HashMapStringStringWritable> {
    	
    	private Text outputKey = new Text();
    	
        @Override
        public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
                   HashMap<String,String>  hm =WeblogFilterUtils.buildKVMap(value.toString());
                   
                   HashMap<String,String>  resulthm = StatisticslogUtils.weblogBuildKVMap(hm);
                   
               	if(resulthm!=null){
	        	   HashMapStringStringWritable outputValue =new HashMapStringStringWritable(resulthm);

        		   outputKey.set(outputValue.getHm().get("url"));
		           context.write(outputKey, outputValue);
	        	}else {
	        		context.getCounter("WeblogToHashMapMapper", "parseLineError").increment(1);
	        	}

			} catch (Exception e) {
				context.getCounter("WeblogToHashMapMapper", "mapException").increment(1);
			}
        }
    }
    
	public static class WeblogToHashMapReducer extends Reducer<Text, HashMapStringStringWritable, Text, HashMapStringStringWritable> {
		
		
        @Override
        protected void reduce(Text key, Iterable<HashMapStringStringWritable> values, Context context) throws IOException, InterruptedException {
       

        	for(HashMapStringStringWritable val : values){
        		context.write(key, val);
        	}
        	       	
        
        }
	}
	
}












