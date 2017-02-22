package com.netease.weblogOffline.statistics.contentscore;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.LongLongWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 通过跟帖的数据，计算当日每篇内容新增跟帖数 及 新增跟帖人数
 * */
public class DailyGenTieCountMR extends MRJob {
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.GEN_TIE_INFO + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "genTieCount/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());

    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), TextInputFormat.class, GenTieInfoMapper.class);
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        //reducer
        job.setReducerClass(CountReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongLongWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class GenTieInfoMapper extends Mapper<LongWritable, Text, Text, Text> {
    	
    	private Text outputKey = new Text();
    	private Text outputValue = new Text();
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
	//        	url,pdocid,docid,发帖用户id,跟帖时间，跟帖id
	        	String[] strs = value.toString().split(",");
	        	String url = strs[0];
	        	String uid = strs[3];
	        	
	        	outputKey.set(url);
	        	outputValue.set(uid);
	        	context.write(outputKey, outputValue);
			} catch (Exception e) {
				context.getCounter("GenTieInfoMapper", "mapException").increment(1);
			}
        }
    }
    
	public static class CountReducer extends Reducer<Text, Text, Text, LongLongWritable> {
		
    	private LongLongWritable outputValue = new LongLongWritable();
    	private Set<String> uidSet = new HashSet<String>();
    	
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	uidSet.clear();
        	long pv = 0;
        	
        	for(Text val : values){
        		pv++;
        		uidSet.add(val.toString());
        	}
        	
        	outputValue.setFirst(pv);
        	outputValue.setSecond(uidSet.size());
        	context.write(key, outputValue);
        }
	}
}












