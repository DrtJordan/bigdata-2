package com.netease.weblogOffline.statistics.dailyWeblog;

import java.io.IOException;
import java.util.HashMap;

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

hql="select refDomain,count(*) from weblog where dt=$yesterday and event=\"launch\" and (url like \"http://www.163.com/%\" or url like \"http://www.netease.com/%\") group by refDomain;"
/bin/bash /home/weblog/.weblogHive $hql | sort -k 2 -nr > $dest/urlIsMainPage_refDomain

hql="select urlDomain,count(*) from weblog where dt=$yesterday and event=\"launch\" and (ref like \"http://www.163.com/%\" or ref like \"http://www.netease.com/%\") group by urlDomain;"
/bin/bash /home/weblog/.weblogHive $hql | sort -k 2 -nr > $dest/refIsMainPage_urlDomain
 * 
 */
public class Weblog163HomeIOMR extends MRJob {
	@Override
	public boolean init(String date) {
      //  inputList.add(DirConstant.WEBLOG_LOG + date);
		inputList.add(DirConstant.WEBLOG_FilterLOG
				+ date);
        outputList.add(DirConstant.WEBLOG_STATISTICS_OTHER_DIR + "163HomeIO/" + date);
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
					
					String ref = lineMap.get("ref");
					
					String refDomain = UrlUtils.urlGetDomain(ref);
					String urlDomain = UrlUtils.urlGetDomain(url);
					
					if(UrlUtils.is163Home(url)){
						outputkey.set("refDomain_" + refDomain);
						context.write(outputkey, outputValue);
					}
					if(UrlUtils.is163Home(ref)){
						outputkey.set("urlDomain_" + urlDomain);
						context.write(outputkey, outputValue);
					}
				}
			} catch (Exception e) {
				context.getCounter("LogMapper", "parseError").increment(1);
			}
        }
    }
    
    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
    	private IntWritable outputValue = new IntWritable();
    	
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum = 0;
        	for(IntWritable v : values){
        		sum += v.get();
        	}
        	outputValue.set(sum);
        	context.write(key, outputValue);
        }
    }
}












