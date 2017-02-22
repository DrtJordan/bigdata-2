package com.netease.weblogOffline.statistics.sessionpath;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import com.netease.weblogOffline.utils.HadoopUtils;

public class FilterResultMR extends MRJob{
	private static int userSumThreshold = 10;
	private static int firstOrderThreshold = 2;
	private static int secondOrderThreshold = 3;
	private static int thirdOrderThreshold = 4;
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.PATH_MIDLAYER_DIR + "pathrecordwithtime/" + date);
 
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "firstorder/" + date );
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "secondorder/" + date );
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "thirdorder/" + date );
     
        return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int jobState = JobConstant.SUCCESSFUL;

        //1.过滤按照时间排序方法获得的数据，取每一个session所对应的路径的前4个跳转页面，即ref->first->sencond->third
        Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");

        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, FilterResultThirdOrderMapper.class);
        
        //mapperr
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job.setReducerClass(FilterResultReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(2)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        //2.过滤按照时间排序方法获得的数据，取每一个session所对应的路径的前3个跳转页面，即ref->first->sencond
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-2");

        MultipleInputs.addInputPath(job1, new Path(outputList.get(2)), SequenceFileInputFormat.class, FilterResultSecondOrderMapper.class);
        
        //mapperr
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job1.setReducerClass(FilterResultReducer.class);
        job1.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(1)));
        //FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        //3.过滤按照时间排序方法获得的数据，取每一个session所对应的路径的前2个跳转页面，即ref->first
        Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-3");

        MultipleInputs.addInputPath(job2, new Path(outputList.get(1)), SequenceFileInputFormat.class, FilterResultFirstOrderMapper.class);
        
        //mapperr
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job2.setReducerClass(FilterResultReducer.class);
        job2.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(0)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
	}
	 public static class FilterResultFirstOrderMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
	    	private IntWritable outputvalue = new IntWritable(0);
	    	private Text outputkey = new Text();
	        @Override
	        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException{
	        	try{
	        		String tempv = null;
	        		String[] tempvalue = null;
	        		String respath = null;
	        		
		        	tempv = key.toString();
		        	tempvalue = tempv.split(",");
		        	 
		        	if(tempvalue.length >= 1){
		        		if(IsIp(tempvalue[0])){
		        			context.getCounter("IpRefGroup","IpRefNums").increment(1);
		        		}else{
		        			respath = tempvalue[0];
		        			if(tempvalue.length == 1){
				        		respath = respath;
				        	}
				        	else if(tempvalue.length > 1 && tempvalue.length <= firstOrderThreshold){
				        		for(int i = 1; i < tempvalue.length; i++){
				        			respath = respath + "," + tempvalue[i];
				        		} 
				        	}
				        	else if(tempvalue.length > firstOrderThreshold){
				        		for(int i = 1; i < firstOrderThreshold; i++){
				        			respath = respath + "," + tempvalue[i];
				        		}
				        	}else{
				        		context.getCounter("FilteringResultError", "FilteringError").increment(1);
				        	}
				        	
				        	try{
				        		outputkey.set(respath);
				        	}catch(Exception e){
				        		context.getCounter("FilteringResultError", "SetOutputkeyError").increment(1);
				        	}
				            context.write(outputkey, value);
		        		}
		        	}else{
		        		respath = "null";
		        		outputkey.set(respath);
		        		context.write(outputkey, outputvalue);
		        	}
		        	
	        	}catch(Exception e){
	        		context.getCounter("FilterResultMapper", "finalresult").increment(1);
	        	}
	        }
	    }
	 
	 public static class FilterResultSecondOrderMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
	    	private IntWritable outputvalue = new IntWritable(0);
	    	private Text outputkey = new Text();
	        @Override
	        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException{
	        	try{
	        		String tempv = null;
	        		String[] tempvalue = null;
	        		String respath = null;
	        		
		        	tempv = key.toString();
		        	tempvalue = tempv.split(",");
		        	
		        	 
		        	if(tempvalue.length >= 1){
		        		if(IsIp(tempvalue[0])){
		        			context.getCounter("IpRefGroup","IpRefNums").increment(1);
		        		}else{
		        			respath = tempvalue[0];
		        			if(tempvalue.length == 1){
				        		respath = respath;
				        	}
				        	else if(tempvalue.length > 1 && tempvalue.length <= secondOrderThreshold){
				        		for(int i = 1; i < tempvalue.length; i++){
				        			respath = respath + "," + tempvalue[i];
				        		} 
				        	}
				        	else if(tempvalue.length > secondOrderThreshold){
				        		for(int i = 1; i < secondOrderThreshold; i++){
				        			respath = respath + "," + tempvalue[i];
				        		}
				        	}else{
				        		context.getCounter("FilteringResultError", "FilteringError").increment(1);
				        	}
				        	
				        	try{
				        		outputkey.set(respath);
				        	}catch(Exception e){
				        		context.getCounter("FilteringResultError", "SetOutputkeyError").increment(1);
				        	}
				            context.write(outputkey, value);
		        		}
		        	}else{
		        		respath = "null";
		        		outputkey.set(respath);
		        		context.write(outputkey, outputvalue);
		        	}
		        	
	        	}catch(Exception e){
	        		context.getCounter("FilterResultMapper", "finalresult").increment(1);
	        	}
	        }
	    }
	 
	 public static class FilterResultThirdOrderMapper extends Mapper<Text, Text, Text, IntWritable> {
	    	private IntWritable outputvalue = new IntWritable(1);
	    	private Text outputkey = new Text();
	        @Override
	        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
	        	try{
	        		String tempv = null;
	        		String[] tempvalue = null;
	        		String respath = null;
	        		
		        	tempv = value.toString();
		        	tempvalue = tempv.split(",");
		        	 
		        	if(tempvalue.length >= 1){
		        		if(IsIp(tempvalue[0])){
		        			context.getCounter("IpRefGroup","IpRefNums").increment(1);
		        		}else{
		        			respath = filterUrlModified(tempvalue[0]);
		        			if(tempvalue.length == 1){
				        		respath = respath;
				        	}
				        	else if(tempvalue.length > 1 && tempvalue.length <= thirdOrderThreshold){
				        		for(int i = 1; i < tempvalue.length; i++){
				        			respath = respath + "," + filterUrl(tempvalue[i]);
				        		} 
				        	}
				        	else if(tempvalue.length > thirdOrderThreshold){
				        		for(int i = 1; i < thirdOrderThreshold; i++){
				        			respath = respath + "," + filterUrl(tempvalue[i]);
				        		}
				        	}else{
				        		context.getCounter("FilteringResultError", "FilteringError").increment(1);
				        	}
				        	
				        	try{
				        		outputkey.set(respath);
				        	}catch(Exception e){
				        		context.getCounter("FilteringResultError", "SetOutputkeyError").increment(1);
				        	}
				            context.write(outputkey, outputvalue);
		        		}
		        	}else{
		        		respath = "null";
		        		outputkey.set(respath);
		        		context.write(outputkey, outputvalue);
		        	}
		        	
	        	}catch(Exception e){
	        		context.getCounter("FilterResultMapper", "finalresult").increment(1);
	        	}
	        }
	    }
	 
	 
	  public static class FilterResultReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		    private IntWritable outputvalue = new IntWritable();
	    	
	    	@Override
	    	public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException{
	    		try{
	    			int sum = 0;
	                for(IntWritable value : values){
	                    sum += value.get();
	                }
	                if(sum < userSumThreshold){
	                	context.getCounter("PathNums_of_UserNumsLessThan10", "PathNums").increment(1);
	                }else{
	                	outputvalue.set(sum);
	                	context.write(key, outputvalue);
	                }
	    		}catch(Exception e){
	    			context.getCounter("FilterResultReducer", "result").increment(1);
	    		}
	    		
	    	}
	  }
	  
	  public static class TestReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		  public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException{
			  for(IntWritable v: values){
				  context.write(key, v);
			  }
		  }
	  }
//	  public static void main(String[] args) {
//		try {
//			URL url = new URL("https://www.baidu.com/s?wd=%E7%8E%9B%E4%B8%BD%E8%8B%8F&rsv_spt=1&issp=1&f=8&rsv_bp=0&rsv_idx=2&ie=utf-8&tn=baiduhome_pg&rsv_enter=1&rsv_sug3=6&rsv_sug1=5");
//			System.out.println(url.getHost());
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	  
	 /**
	     * 去掉www或者com的后缀
	     * @param URL
	     * @return
	     */
	  private static String filterUrlModified(String URL){
		  String filterurl = URL == null ? null : URL.toLowerCase();
		  
		  try{
			  if(filterurl!=null && filterurl.length()>0){
				  URL resURL = new URL(filterurl);
				  filterurl = resURL.getHost();
			  }
		  }catch(Exception e){
			  e.printStackTrace();
		  }
		  return filterurl;
	  }
	  
	  	private static String EmitSpace(String IP){//去掉IP字符串前后所有的空格  
	        while(IP.startsWith(" ")){  
	               IP= IP.substring(1,IP.length()).trim();  
	            }  
	        while(IP.endsWith(" ")){  
	               IP= IP.substring(0,IP.length()-1).trim();  
	            }  
	        return IP;  
	    }  
	      
	    private static boolean IsIp(String IP){//判断是否是一个IP  
	        boolean isIp = false;  
	        IP = EmitSpace(IP);  
	        if(IP.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")){  
	            String s[] = IP.split("\\.");  
	            if(Integer.parseInt(s[0])<255)  
	                if(Integer.parseInt(s[1])<255)  
	                    if(Integer.parseInt(s[2])<255)  
	                        if(Integer.parseInt(s[3])<255)  
	                            isIp = true;  
	        }else if(IP.contains(":")){
	        	isIp = true;
	        }
	        return isIp;  
	    }  
	    
	    private static String filterUrl(String URL){
	        String filterurl = URL==null? null: URL.toLowerCase();
	        if(filterurl!=null && filterurl.length()>0){
	            if(filterurl.endsWith(".")){
	                filterurl = filterurl.substring(0, filterurl.length()-1);
	            }
	            if(filterurl.contains(".cz")
	                || filterurl.contains(".com")
	                || filterurl.contains(".cn")
	                || filterurl.contains(".org")
	                || filterurl.contains(".edu")
	                || filterurl.contains(".net")){

	                filterurl = getFilterUrl(filterurl, ".cz");
	                filterurl = getFilterUrl(filterurl, ".com");
	                filterurl = getFilterUrl(filterurl, ".cn");
	                filterurl = getFilterUrl(filterurl, ".org");
	                filterurl = getFilterUrl(filterurl, ".edu");
	                filterurl = getFilterUrl(filterurl, ".net");
	                if(filterurl.contains(".")){
	                    filterurl = filterurl.substring(filterurl.lastIndexOf(".")+1);
	                }
	            }
	        }
	        return filterurl;
	    }

	    private static String getFilterUrl(String filterurl, String rule) {
	        if(filterurl.endsWith(rule)){
	            filterurl = filterurl.substring(0, filterurl.lastIndexOf(rule));
	        }
	        return filterurl;
	    }
}
