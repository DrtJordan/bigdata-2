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
import com.netease.weblogOffline.data.StringStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

public class HighQualityUserRecordMR extends MRJob{
	private static int userSumThreshold = 10;
	private static int pathThreshold = 6;//包含ref,也就是说在网易站内访问的页面数应当至少为5
	
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.PATH_MIDLAYER_DIR + "pathrecordwithtime/" + date);
 
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "highqualityuserrecord/" + date );
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "highqualityuserrecordwithpathnums/" + date );
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "highqualityuserrecordcombined/" + date );
        
     
        return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int jobState = JobConstant.SUCCESSFUL;

        //1.统计不同ref下的优质用户的数量，当页面访问跳转数量超过5页则判定该用户为优质用户，从而看出哪个外站能提供的优质用户较为多，结果：（ref||用户数）
        Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");

        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, HighQualityUserRecordMapper.class);
        
        //mapperr
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job.setReducerClass(HighQualityUserRecordReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        
        //2.与1的功能相近，只是同时记录了用户在通过某外站进入网易之后访问了包括登陆页在内的页面数，结果：（ref,页面数（包括登陆页，不含ref）||用户数）
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-2");

        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, HighQualityUserRecordWithPathNumsMapper.class);
        
        //mapperr
        job1.setMapOutputKeyClass(StringStringWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job1.setReducerClass(HighQualityUserRecordWithPathNumsReducer.class);
        job1.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(1)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setOutputKeyClass(StringStringWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
      //3.与1的功能相近，只是解析ref的力度更细致，只要为相同Host的页面即为相同，eg：0.baidu.com与www.baidu.com均为baidu，从而将通过移动设备与网站设备进入网易的用户合并
        Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-3");

        MultipleInputs.addInputPath(job2, new Path(inputList.get(0)), SequenceFileInputFormat.class, HighQualityUserRecordCombinedMapper.class);
        
        //mapperr
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job2.setReducerClass(HighQualityUserRecordReducer.class);
        job2.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(2)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        return jobState;
	}
	 
	 public static class HighQualityUserRecordMapper extends Mapper<Text, Text, Text, IntWritable> {
	    	private IntWritable outputvalue = new IntWritable(1);
	    	private Text outputkey = new Text();
	        @Override
	        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
	        	try{
	        		String tempv = null;
	        		String[] tempvalue = null;
	        		String resref = null;
	        		
		        	tempv = value.toString();
		        	tempvalue = tempv.split(",");
		        	 
		        	if(tempvalue.length >= pathThreshold){ 
		        		if(IsIp(tempvalue[0])){
		        			context.getCounter("IpRefGroup","IpRefNums").increment(1);
		        		}
		        		else{
		        			resref = filterUrlModified(tempvalue[0]);
				        	
				        	try{
				        		outputkey.set(resref);
				        	}catch(Exception e){
				        		context.getCounter("FilteringResultError", "SetOutputkeyError").increment(1);
				        	}
				            context.write(outputkey, outputvalue);
		        		}
		        	}else{
		        		context.getCounter("BadPathGroup","TotalAmount").increment(1);
		        	}
		        	
	        	}catch(Exception e){
	        		context.getCounter("FilterResultMapper", "finalresult").increment(1);
	        	}
	        }
	    }
	 
	 public static class HighQualityUserRecordWithPathNumsMapper extends Mapper<Text, Text, StringStringWritable, IntWritable> {
	    	private IntWritable outputvalue = new IntWritable(1);
	    	private StringStringWritable outputkey = new StringStringWritable();
	        @Override
	        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
	        	try{
	        		String tempv = null;
	        		String[] tempvalue = null;
	        		String res = null;
	        		
		        	tempv = value.toString();
		        	tempvalue = tempv.split(",");
		        	 
		        	if(tempvalue.length >= pathThreshold){
		        		if(IsIp(tempvalue[0])){
		        			context.getCounter("IpRefGroup","IpRefNums").increment(1);
		        		}
		        		else{
				        	try{
				        		outputkey.setFirst(filterUrlModified(tempvalue[0])); 
			        			outputkey.setSecond(String.valueOf(tempvalue.length - 1));
				        	}catch(Exception e){
				        		context.getCounter("FilteringResultError", "SetOutputkeyError").increment(1);
				        	}
				        	
				            context.write(outputkey, outputvalue);
		        		}
		        	}else{
		        		context.getCounter("BadPathGroup","TotalAmount").increment(1);
		        	}
		        	
	        	}catch(Exception e){
	        		context.getCounter("FilterResultMapper", "finalresult").increment(1);
	        	}
	        }
	    }
	 
	 //下列mapper是对job1的结果直接进行整合，相较于直接解析inputlist中的数据在速度上要快很多，结果与直接解析Inputlist的结果数据相差不大，建议使用此方法
//	 public static class HighQualityUserRecordCombinedMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
//	    	//private IntWritable outputvalue = new IntWritable();
//	    	private Text outputkey = new Text();
//	        @Override
//	        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException{
//	        	try{
//	        		String tempk = null;
//	        		String resref = null;
//	        		
//	        		tempk = key.toString();
//		        	if(IsIp(tempk)){
//		        		context.getCounter("IpRefGroup","IpRefNums").increment(1);
//		        	}
//		        	else{
//		        		resref = filterUrl(tempk);
//				        	
//				        try{
//				        	outputkey.set(resref);
//				        }catch(Exception e){
//				        	context.getCounter("FilteringResultError", "SetOutputkeyError").increment(1);
//				        }
//				           context.write(outputkey, value);
//		        	}
//		        	
//		        	
//	        	}catch(Exception e){
//	        		context.getCounter("FilterResultMapper", "finalresult").increment(1);
//	        	}
//	        }
//	    }
	 
	 //下列mapper直接解析inputlist中的数据，运行速度较慢，不建议用此方法
	 public static class HighQualityUserRecordCombinedMapper extends Mapper<Text, Text, Text, IntWritable> {
	    	private IntWritable outputvalue = new IntWritable(1);
	    	private Text outputkey = new Text();
	        @Override
	        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
	        	try{
	        		String tempv = null;
	        		String[] tempvalue = null;
	        		String resref = null;
	        		
		        	tempv = value.toString();
		        	tempvalue = tempv.split(",");
		        	 
		        	if(tempvalue.length >= pathThreshold){ 
		        		if(IsIp(tempvalue[0])){
		        			context.getCounter("IpRefGroup","IpRefNums").increment(1);
		        		}
		        		else{
		        			resref = filterUrl(tempvalue[0]);
				        	
				        	try{
				        		outputkey.set(resref);
				        	}catch(Exception e){
				        		context.getCounter("FilteringResultError", "SetOutputkeyError").increment(1);
				        	}
				            context.write(outputkey, outputvalue);
		        		}
		        	}else{
		        		context.getCounter("BadPathGroup","TotalAmount").increment(1);
		        	}
		        	
	        	}catch(Exception e){
	        		context.getCounter("FilterResultMapper", "finalresult").increment(1);
	        	}
	        }
	    }
	 
	  public static class HighQualityUserRecordReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		    private IntWritable outputvalue = new IntWritable();
	    	
	    	@Override
	    	public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException{
	    		try{
	    			int sum = 0;
	                for(IntWritable value : values){
	                    sum += value.get();
	                }
	                if(sum <= userSumThreshold){
	                	context.getCounter("RefNums_of_UserNumsLessThan10", "PathNums").increment(1);
	                }else{
	                	outputvalue.set(sum);
	                	context.write(key, outputvalue);
	                }
	    		}catch(Exception e){
	    			context.getCounter("FilterResultReducer", "result").increment(1);
	    		}
	    		
	    	}
	  }
	  
	  public static class HighQualityUserRecordWithPathNumsReducer extends Reducer<StringStringWritable,IntWritable,StringStringWritable,IntWritable> {
		    private IntWritable outputvalue = new IntWritable();
	    	
	    	@Override
	    	public void reduce(StringStringWritable key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException{
	    		try{
	    			int sum = 0;
	                for(IntWritable value : values){
	                    sum += value.get();
	                }
	                if(sum <= userSumThreshold){
	                	context.getCounter("RefNums_of_UserNumsLessThan10", "PathNums").increment(1);
	                }else{
	                	outputvalue.set(sum);
	                	context.write(key, outputvalue);
	                }
	    		}catch(Exception e){
	    			context.getCounter("FilterResultReducer", "result").increment(1);
	    		}
	    		
	    	}
	  }
	
	  
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
