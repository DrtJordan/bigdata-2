package com.netease.weblogOffline.temp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.logparsers.ZyLogParams;
import com.netease.weblogCommon.logparsers.ZylogParser;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.temp.PhotoSetUrlOfGivenOriginMR.PhotoSetIncrMapper;
import com.netease.weblogOffline.utils.HadoopUtils;

//tofix
public class PhotoUrlPvFromZyMR extends MRJob {
	@Override
	public boolean init(String date) {
		
		List<String> dateList;
		try {
			String firstDay = DateUtils.getTheDayBefore(date, 39);
			dateList = DateUtils.getDateList(firstDay, date);
		} catch (ParseException e) {
			return false;
		}
		for (String d : dateList) {
			inputList.add(DirConstant.ZY_LOG+d);
		}
		
		outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR+"PhotoUrlPvFromZy/"+date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());
    	  for (String input : inputList) {
              Path path = new Path(input);
              if (getHDFS().exists(path)) {
            	  MultipleInputs.addInputPath(job, path, TextInputFormat.class, LogMapper.class);
              }
    	  }
        
      
        DistributedCache.addCacheFile(new URI(getHDFS().getUri().toString() +"/ntes_weblog/weblog/statistics/temp/gongshaojie1"+"#shaojieFile"), job.getConfiguration());
        DistributedCache.createSymlink(job.getConfiguration());
        
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setCombinerClass(photoPvReduce.class);
        
        //reducer
        job.setReducerClass(photoPvReduce.class);
        job.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.getConfiguration().setLong("mapreduce.jobtracker.split.metainfo.maxsize", -1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    	
    	private Text outputkey = new Text();
    	private static List<String>  list;
    	private IntWritable outputValue = new IntWritable(1);
    	
    	private LogParser logParser = new ZylogParser(); 
    	
        @Override
   		protected void setup(Context context) throws IOException,
   				InterruptedException {
        	list = loadCacheFileToList(context);
   		}
        private List<String> loadCacheFileToList(Context context) throws IOException {
			List<String> al = new ArrayList<String>();
			
			BufferedReader br = null;
		    br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("shaojieFile"))));
				 
				 String str = null;
				 str = br.readLine();//escape first line
				 while ((str = br.readLine())!=null){
					 try {
						 String url = str; 
						 al.add(url);
					 }catch(Exception e ){
						context.getCounter("InfoOfPerPGRMapper", "loadCacheFileToMap").increment(1);
					 }

				 }
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return al;
		}
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
				Map<String, String> logMap = logParser.parse(value.toString());
				String url = logMap.get(ZyLogParams.url);
				String pureUrl = UrlUtils.getOriginalUrl(url);
				 if( UrlUtils.isPhotoset(url)){
					 if(list.contains(pureUrl)){
						 outputkey.set(url);
						 context.write(outputkey, outputValue);
					 }
				 }
				
			   
				
			} catch (Exception e) {
				context.getCounter("LogMapper", "parseError").increment(1);
			}
        }
        
       
    }
    
	public static class photoPvReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
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
    
}












