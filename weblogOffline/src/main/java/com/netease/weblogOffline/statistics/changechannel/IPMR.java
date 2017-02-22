package com.netease.weblogOffline.statistics.changechannel;

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
import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.weblogfilter.WeblogFilterUtils;
import com.netease.weblogOffline.data.ThreeStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;


/**
 * IP 分布
 *
 */

public class IPMR extends MRJob {

	@Override
	public boolean init(String date) {
        //输入列表
		inputList.add(DirConstant.WEBLOG_FilterLOG
				+ date);

        outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR +"IPtemp/" + date);
        outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR +"IP/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
    	
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName());
        
        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, IPMapper.class);
        
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(ThreeStringWritable.class);
        

        //reducer
        job1.setReducerClass(IPReducer.class);
        job1.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }    
        Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass()
				.getName() + "-step2");

		MultipleInputs.addInputPath(job2, new Path(outputList.get(0)),
				SequenceFileInputFormat.class,
				IP2Mapper.class);

		// mapper
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		// reducer
		job2.setReducerClass(IP2Reducer.class);
		job2.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		if (!job2.waitForCompletion(true)) {
			jobState = JobConstant.FAILED;
		}

        

        return jobState;
    }
    
    
    public static class IPMapper extends
	                 Mapper<NullWritable, Text, Text, ThreeStringWritable> {
   

		@Override
        public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
 	     	try {
    		HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());
    	//	SimpleDateFormat sdf = 	new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);
       	//	String lineMap[] = value.toString().split("\t");
       	            String ip = lineMap.get("ip");
       	     	    String url =lineMap.get("url");
       	     	    NeteaseChannel_CS nce = NeteaseChannel_CS.getChannel(url);      
       	     		String event = lineMap.get("event");
       	     	    String   serverTime =lineMap.get("serverTime");
       	     	    
       	        	String str[] = lineMap.get("project").split("@version@");
       	        	String uuid = lineMap.get("uuid");
       	     	if (str.length==2){
       	        	String   broad = str[1];
       	     	if(broad.indexOf("_")==-1){
       				String[] broadtime =broad.split("-");
       	     //   	long needTime = SimpleDateFormatEnum.weblogServerTimeFormat.get().parse("18/Aug/2015:15:10:00").getTime();
       				
                    if (event.equals("launch")&&nce.getName().equals("travel")&&NeteaseContentType.artical.match(url)){
                    	  ThreeStringWritable tsw =new ThreeStringWritable(broadtime[0],ip,serverTime);
                    	  context.write(new Text(uuid),tsw);
                    }
       	     	}
       	     	}
      
			
            } catch (Exception e) {
				context.getCounter("InfoOfPerPGRMapper", "parseError").increment(1);
			}
        }

    }
    

       public static class IPReducer extends Reducer<Text, ThreeStringWritable, Text, Text> {
         Text outValue = new Text();
         Text outKey = new Text();
    	@Override
		protected void reduce(Text key, Iterable<ThreeStringWritable> values, Context context) throws IOException, InterruptedException {
    		long time = Long.MAX_VALUE;
    		 ThreeStringWritable tsw =new ThreeStringWritable();
		    for (ThreeStringWritable val :values){
		    	if(Long.parseLong(val.getthird())<time){
		    		time =Long.parseLong(val.getthird());
		    		tsw = new ThreeStringWritable(val.getfirst(),val.getsecond(),val.getthird());
		    	}
		    }
		    outKey.set(tsw.getsecond());
		    outValue.set(tsw.getfirst());
		    context.write(outKey,outValue);	
		  
		}
    }
      
       public static class IP2Mapper extends Mapper<Text, Text, Text,Text> {
    	   

   		@Override
           public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
          	try {
      
                       context.write(key,value);
   			
               } catch (Exception e) {
   				context.getCounter("InfoOfPerPGRMapper", "parseError").increment(1);
   			}
           }

       }
       

        public static class IP2Reducer extends Reducer<Text, Text, Text, IntWritable> {
           HashMap<String,Integer> hm = new HashMap<String,Integer>(); 
           IntWritable outValue = new IntWritable();
       	@Override
   		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	try {  
       		  String ipend = key.toString().split("_")[0].split("\\.")[3];
       		  int s = (Integer.parseInt(ipend)%10);
       		  for(Text val :values){
       			  String ss = val.toString()+"_"+s;
           		  if (hm.get(ss)==null){
             			hm.put(ss, 1);
             		  }else {
             			hm.put(ss, hm.get(ss)+1);
             		 } 
       		  }

        	}catch (Exception e) {
    				
    			}
       		  
   		}
		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
//			 for (String ipend :hm.keySet()){
//				 outValue.set(hm.get(ipend));
//				 context.write(new Text(ipend), outValue);
//			 }
			
			for (int i=0;i<10;i++){
				if (hm.get("a1508_"+i)!=null){
					 outValue.set(hm.get("a1508_"+i));
    				 context.write(new Text("a1508_"+i), outValue);
				}else {
					 outValue.set(0);
    				 context.write(new Text("a1508_"+i), outValue);
				}
				
				if (hm.get("a1301_"+i)!=null){
					 outValue.set(hm.get("a1301_"+i));
   				 context.write(new Text("a1301_"+i), outValue);
				}else {
					 outValue.set(0);
   				 context.write(new Text("a1301_"+i), outValue);
				}
			}
		}
       	
       }
         
       
       
}





