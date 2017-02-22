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
 * Channel broad click事件 Url
 *
 */

public class ChannelClickUrlMR extends MRJob {

	@Override
	public boolean init(String date) {
        //输入列表
		inputList.add(DirConstant.WEBLOG_FilterLOG
				+ date);
        outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR +"ChannelClickUrl/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
    	
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());
        
        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, ChannelClickUrlMapper.class);
        
        //mapper
        job.setMapOutputKeyClass(ThreeStringWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        

        //reducer
        job.setReducerClass(ChannelClickUrlReducer.class);
        job.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(ThreeStringWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }    

        return jobState;
    }
    
    
    public static class ChannelClickUrlMapper extends Mapper<NullWritable, Text, ThreeStringWritable,IntWritable> {
   
         IntWritable outValue = new IntWritable(-1);

		@Override
        public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
       	try {
       		HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());
       	            String event = lineMap.get("event");
       	        	String url = lineMap.get("url");
       	    		NeteaseChannel_CS nce = NeteaseChannel_CS.getChannel(url);
				if (event.equals("click")&&NeteaseContentType.artical.match(url)&&nce!=null){
			
					String str[] = lineMap.get("project").split("@version@");
		
					if (str.length==2){
						String	broad = str[1];
						if(broad.indexOf("_")==-1){
					ThreeStringWritable tsw =new ThreeStringWritable(nce.getName(),broad,url);
					
                    context.write(tsw,outValue);
						}
					}
				}
            } catch (Exception e) {
				context.getCounter("InfoOfPerPGRMapper", "parseError").increment(1);
			}
        }

    }
    

       public static class ChannelClickUrlReducer extends Reducer<ThreeStringWritable, IntWritable, ThreeStringWritable, IntWritable> {
        
    	@Override
		protected void reduce(ThreeStringWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
			for (IntWritable v : values) {
				 context.write(key,v);
			}
		  
		}
    }
      

       
}





