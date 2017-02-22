package com.netease.weblogOffline.temp;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.logparsers.WeblogParser;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
hql="select event,ptype,pver,count(uuid),count(distinct uuid) from weblog where dt=$yesterday and (url=\"http://www.163.com/\" or url like \"http://www.163.com/#%\") and (event like \"launch%\" or event like \"initializ%\") and ((ptype=\"ab_0\" and pver=\"b\") or (ptype=\"ab_1\" and pver=\"c\") or (ptype=\"ab_2\" and pver=\"d\") or (ptype=\"ab_3\" and pver=\"e\") or (ptype=\"ab_4\" and pver=\"f\") or (ptype=\"\" and pver=\"\") or (ptype=\"\" and pver=\"a\") ) group by event,ptype,pver;"
/bin/bash /home/weblog/.weblogHive $hql | sort -k 4 -nr | awk '{print "'$yesterday'",$0}'> $dest/homeABTest
 * 
 */
public class ABTestPvUvMR extends MRJob {
	@Override
	public boolean init(String date) {
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());

    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), TextInputFormat.class, LogMapper.class);
        
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setCombinerClass(Combiner.class);
        
        //reducer
        job.setReducerClass(CountReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    	
    	private Text outputkey = new Text();
    	private IntWritable outputValue = new IntWritable(1);
    	private LogParser logParser = new WeblogParser(); 
    	
    	protected void setup(Context context1) throws IOException, InterruptedException {
    		InputSplit inputSplit = context1.getInputSplit();
    		((FileSplit)inputSplit).getPath().getParent();
    	}
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
				Map<String, String> logMap = logParser.parse(value.toString());
				String event = logMap.get("event");
				if("launch".equals(event)){
					String url = logMap.get("url");
					String uuid = logMap.get("uuid");
					String email = logMap.get("cvar_email");
					
					if(UrlUtils.is163Home(url)){
						outputkey.set("163home_" + uuid);
						context.write(outputkey, outputValue);
					}
					if(StringUtils.isNotBlank(email)){
						outputkey.set("validEmail_" + uuid);
						context.write(outputkey, outputValue);
					}
				}
			} catch (Exception e) {
				context.getCounter("LogMapper", "parseError").increment(1);
			}
        }
    }
    
	public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable outputValue = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable v : values) {
				sum += v.get();
			}
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}
    
    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
    	private Map<String, Integer> pvMap = new TreeMap<String, Integer>();
    	private Map<String, Integer> uvMap = new TreeMap<String, Integer>();
        
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	String k = key.toString();
        	String prefix = k.substring(0, k.indexOf("_") + 1);
        	
        	int sum = 0;
			for (IntWritable v : values) {
				sum += v.get();
			}
			
			Integer pv = pvMap.get(prefix);
			if(null == pv){
				pv = 0;
			}
			pvMap.put(prefix, pv + sum);
			
			Integer uv = uvMap.get(prefix);
			if(null == uv){
				uv = 0;
			}
			uvMap.put(prefix, uv + 1);
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for(Entry<String, Integer> e : pvMap.entrySet()){
        		context.write(new Text(e.getKey() + "pv"), new IntWritable(e.getValue()));
        	}
        	for(Entry<String, Integer> e : uvMap.entrySet()){
        		context.write(new Text(e.getKey() + "uv"), new IntWritable(e.getValue()));
        	}
    	}
    }
}












