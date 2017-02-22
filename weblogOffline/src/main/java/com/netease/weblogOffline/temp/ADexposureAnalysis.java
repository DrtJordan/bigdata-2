package com.netease.weblogOffline.temp;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.weblogfilter.WeblogFilterUtils;
import com.netease.weblogOffline.data.IntLongWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 统计广告的曝光次数
 *
 */

public class ADexposureAnalysis extends MRJob {

	@Override
	public boolean init(String date) {
        //输入列表
      //  inputList.add(DirConstant.WEBLOG_LOG + date);//weblog
		inputList.add(DirConstant.WEBLOG_FilterLOG
				+ date);
		// inputList.add(DirConstant.WEBLOG_LOG + "test");//weblog
        //输出列表
        outputList.add("/ntes_weblog/weblog/temp/ADexposureAnalysis/temp/" + date);
        outputList.add("/ntes_weblog/weblog/temp/ADexposureAnalysis/count/" + date);

		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
    	
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");
        
        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, InfoOfPerPGRMapper.class);
        
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntLongWritable.class);
        DistributedCache.addCacheFile(new URI(getHDFS().getUri().toString() +"/ntes_weblog/weblog/temp/ADexposureAnalysis/CacheFile/ad.txt"+"#ADCacheFile"), job1.getConfiguration());
        DistributedCache.createSymlink(job1.getConfiguration());
        
        //reducer
        job1.setReducerClass(InfoOfPerPGRReducer.class);
        job1.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
    	Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step2");
        
        MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, CountMapper.class);
        
        //mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        
        job2.setCombinerClass(Combiner.class);
        
        //reducer
        job2.setReducerClass(CountReducer.class);
        job2.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    
    public static class InfoOfPerPGRMapper extends Mapper<NullWritable, Text, Text,IntLongWritable> {
   
    	private static HashMap<String,Integer>  map;
    	private Text outKey=  new Text();
    	private IntLongWritable outValue=  new IntLongWritable();
    	private static final String SPILITER= "_";
    	
        @Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
	      map = loadCacheFileToMap(context);
		}

		@Override
        public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
       	try {
       		       HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());
             		String url =lineMap.get("url");
             		 String event = lineMap.get("event");
				if (url.equals("http://www.163.com/")&&event.equals("viewFocus")){
				     long utime = Long.parseLong(lineMap.get("utime"));
				     
					float pagescrolly = Float.parseFloat(lineMap.get("pagescrollyy"));
					float avlbsizey =  Float.parseFloat(lineMap.get("avlbsizeyy"));
					String pgr =lineMap.get("pgr");
			
					
			        for (String mapKey :map.keySet()){
			        	
		            	outKey.set(pgr+SPILITER+mapKey);
			            if(((pagescrolly-45)>=(map.get(mapKey)-avlbsizey))&&((pagescrolly-45)<=map.get(mapKey))){
			            	outValue.setFirst(1);
			            	outValue.setSecond(utime);
			            	context.write(outKey,outValue);
		                }else {
		                	outValue.setFirst(0);
			            	outValue.setSecond(utime);		
			            	context.write(outKey,outValue);
		                }
			        }
				}
				
            
            } catch (Exception e) {
				context.getCounter("InfoOfPerPGRMapper", "parseError").increment(1);
			}
        }
		
		
		private HashMap<String,Integer> loadCacheFileToMap(Context context) throws IOException {
			HashMap<String,Integer> hm = new HashMap<String,Integer>();
			
			BufferedReader br = null;
		    br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("ADCacheFile"))));
				 
				 String str = null;
				 str = br.readLine();//escape first line
				 while ((str = br.readLine())!=null){
					 try {
						 String[] adinfo = str.split("\\s+"); 
						 hm.put(adinfo[0], (int)Float.parseFloat(adinfo[3]));
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
			return hm;
		}
    }
    
    public static class InfoOfPerPGRReducer extends Reducer<Text,IntLongWritable, Text, IntWritable> {
    	
    	
    	private Text outKey = new Text();
    	private IntWritable  outValue= new IntWritable();
    	
    
        @Override
        protected void reduce(Text key, Iterable<IntLongWritable> values, Context context) throws IOException, InterruptedException {
            
        	TreeSet<IntLongWritable> ts = new  TreeSet<IntLongWritable>();
        	for(IntLongWritable val : values){
        		ts.add(new IntLongWritable(val));
        	}
        	
        	int sum =0;
        	Iterator<IntLongWritable> it = ts.iterator();
        	IntLongWritable  pre = new IntLongWritable(0,0);
        	    while(it.hasNext()){
            		 IntLongWritable  cur = it.next();
            		 if (cur.getFirst()==1&&pre.getFirst()!=cur.getFirst()){
            			 sum++;
            		 }
            			 pre = cur;
           		  }
	       outKey.set(key.toString().split("_")[1]);
	       outValue.set(sum);   
           context.write(outKey, outValue);
        }

    }
    
    public static class CountMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
      
    	
        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        	
        	context.write(key, value);
        }
    }

    
	public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        
		private IntWritable  outValue= new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable v : values) {
				sum += v.get();
			}
			
		       outValue.set(sum);  
			context.write(key, outValue);
		}
	}
    
        public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
        	private IntWritable  outValue= new IntWritable();
        
    	@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable v : values) {
				sum += v.get();
			}
		    outValue.set(sum);  
		    context.write(key, outValue);
		}

    }
}





