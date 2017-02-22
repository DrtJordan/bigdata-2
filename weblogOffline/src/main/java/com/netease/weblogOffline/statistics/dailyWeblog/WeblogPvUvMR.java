package com.netease.weblogOffline.statistics.dailyWeblog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
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

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.weblogfilter.WeblogFilterUtils;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
hql="select count(uuid),count(distinct uuid) from weblog where dt=$yesterday and event=\"launch\" and (url like \"http://www.163.com/%\" or url like \"http://www.netease.com/%\");"
/bin/bash /home/weblog/.weblogHive "$hql" | awk '{print "pv="$1", uv="$2}'> $dest/mainPagePvUv

hql="select count(uuid),count(distinct uuid) from weblog where dt=$yesterday and event=\"launch\" and email!=\"\";"
/bin/bash /home/weblog/.weblogHive $hql | awk '{print "pv="$1", uv="$2}' > $dest/validMailPvUv
 * 
 */
public class WeblogPvUvMR extends MRJob {
	@Override
	public boolean init(String date) {
	//	inputList.add(DirConstant.WEBLOG_LOG + date);
		inputList.add(DirConstant.WEBLOG_FilterLOG
				+ date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_TODC_DIR + "pvuv/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());
        
        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, LogMapper.class);
        
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
    
    public static class LogMapper extends Mapper<NullWritable, Text, Text, IntWritable> {
    	
    	private Text outputkey = new Text();
    	private IntWritable outputValue = new IntWritable(1);
    	
    	
        @Override
        public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
    			HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());
				String event = lineMap.get("event");
				if("launch".equals(event)){
					

			    	String url = lineMap.get("url");
					String uuid = lineMap.get("uuid");
					String email = lineMap.get("cvar_email");
					
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












