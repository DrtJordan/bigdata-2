package com.netease.weblogOffline.temp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

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
import com.netease.weblogOffline.data.CountOfBouncePageWritable;
import com.netease.weblogOffline.data.StringStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 统计广告的曝光次数
 *
 */

public class BouncePageTestbyPureURLMR extends MRJob {

	@Override
	public boolean init(String date) {
        //输入列表
    //    inputList.add(DirConstant.WEBLOG_LOG + date);//weblog
		inputList.add(DirConstant.WEBLOG_FilterLOG
				+ date);
		// inputList.add(DirConstant.WEBLOG_LOG + "test");//weblog
        //输出列表
        outputList.add("/ntes_weblog/weblog/temp/BouncePageTestMR/tempbyurl/" + date);
        outputList.add("/ntes_weblog/weblog/temp/BouncePageTestMR/resultbyurl/" + date);
        outputList.add("/ntes_weblog/weblog/temp/BouncePageTestMR/sortresultbyurl/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
    	
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");
        
        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, InfoOfPerSIDMapper.class);
        
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(StringStringWritable.class);
        

        //reducer
        job1.setReducerClass(InfoOfPerSIDReducer.class);
        job1.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }    
        
        Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step2");
        
        MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, CountPerUrlMapper.class);
        
        //mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        
        job2.setCombinerClass(CountPerUrlcombine.class);
        
        //reducer
        job2.setReducerClass(CountPerUrlReducer.class);
        job2.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job2.setOutputKeyClass(CountOfBouncePageWritable.class);
        job2.setOutputValueClass(Text.class);
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }

        
       Job job3 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step3");
        
        MultipleInputs.addInputPath(job3, new Path(outputList.get(1)), SequenceFileInputFormat.class, SortMapper.class);
        
        //mapper
        job3.setMapOutputKeyClass(CountOfBouncePageWritable.class);
        job3.setMapOutputValueClass(Text.class);
        
        
        //reducer
        job3.setReducerClass(SortReducer.class);
        job3.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job3, new Path(outputList.get(2)));
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job3.setOutputKeyClass(CountOfBouncePageWritable.class);
        job3.setOutputValueClass(Text.class);
        
        if(!job3.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        return jobState;
    }
    
    
    public static class InfoOfPerSIDMapper extends Mapper<NullWritable, Text, Text,StringStringWritable> {
   

    	private StringStringWritable gprAndPureUrl= new StringStringWritable();
    	
		@Override
        public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
       	try {
       	      HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());
 
       	            String event = lineMap.get("event");
				if (event.equals("launch")){
		
					String sid = lineMap.get("sid");
					String url = lineMap.get("url");
					String pureUrl = UrlUtils.getOriginalUrl(url);
			        String pgr = lineMap.get("pgr");
			        gprAndPureUrl.setFirst(pgr);
			        gprAndPureUrl.setSecond(pureUrl);
                    context.write(new Text(sid), gprAndPureUrl);
				}
            } catch (Exception e) {
				context.getCounter("InfoOfPerPGRMapper", "parseError").increment(1);
			}
        }

    }
    

       public static class InfoOfPerSIDReducer extends Reducer<Text, StringStringWritable, Text, IntWritable> {
    	
        
    	@Override
		protected void reduce(Text key, Iterable<StringStringWritable> values, Context context) throws IOException, InterruptedException {
		
		    HashSet<String> hspureurl =new HashSet<String>();
		    HashSet<String> hspgr =new HashSet<String>();
			for (StringStringWritable v : values) {
				String pgr = v.getFirst();
				String pureUrl = v.getSecond();
				if (pureUrl!=""){
					hspureurl.add(pureUrl);
				}
				if (pgr!=""){
					hspgr.add(pgr);
				}

			}
			
		    if (hspgr.size()==1&&hspureurl.size()==1){
		    	  context.write(new Text(hspureurl.toArray()[0].toString()),new IntWritable(1));
		    }
		  
		}
    }
      
       public static class CountPerUrlMapper extends Mapper<Text,IntWritable , Text,IntWritable> {
    	   
	 	private static final List<String> vaildPrefixs = new ArrayList<String>();
    	static {
    		vaildPrefixs.add("play.163.com");
    		vaildPrefixs.add("yuedu.163.com");
    		vaildPrefixs.add("game.163.com");
    		vaildPrefixs.add("open.163.com");
    		vaildPrefixs.add("//a.163.com");
    		vaildPrefixs.add("//d.163.com");
    		vaildPrefixs.add("//w.163.com");
    		vaildPrefixs.add("//s.163.com");
    		vaildPrefixs.add("cai.163.com");
    		vaildPrefixs.add("book.163.com");
    	}
   		@Override
           public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
   			
   			if (isValidUrl(key.toString())){
   				context.write(key,value);     	
   			}
   			
           }
   		
		private static boolean isValidUrl(String url){
			for(String regex : vaildPrefixs){
				
				if(url.indexOf(regex)!=-1){
					return false;
				}
			}		
			return true;
		}

       }  
       
       
       public static class CountPerUrlcombine extends Reducer<Text, IntWritable, Text, IntWritable> {
          	
           private IntWritable result = new IntWritable();
          	@Override
      		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      			int sum =0;
      			
      			for (IntWritable v : values) {
      			  sum+=v.get();
      			}
      			result.set(sum);
      			context.write(key,result);
      		}
          }
       
       public static class CountPerUrlReducer extends Reducer<Text, IntWritable, CountOfBouncePageWritable, Text> {
       	
    	 private  CountOfBouncePageWritable cabp =  new CountOfBouncePageWritable();

       	@Override
   		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       		int sum =0;
  			
  			for (IntWritable v : values) {
  			  sum+=v.get();
  			}
  			cabp.setCount(sum);
  			context.write(cabp,key);
   		}
       }
       
       public static class SortMapper extends Mapper<CountOfBouncePageWritable,Text , CountOfBouncePageWritable,Text> {
    	   
          	
      		@Override
              public void map(CountOfBouncePageWritable key, Text value, Context context) throws IOException, InterruptedException {
      			context.write(key,value);     
              }

          }  
       public static class  SortReducer extends Reducer<CountOfBouncePageWritable, Text, CountOfBouncePageWritable, Text> {
          	
    
         	@Override
     		protected void reduce(CountOfBouncePageWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    			
    			for (Text v : values) {
    				context.write(key,v);
    			}

     		}
         }  
       
}





