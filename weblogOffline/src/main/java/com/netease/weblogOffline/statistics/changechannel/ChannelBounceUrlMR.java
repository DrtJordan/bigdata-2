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
 * Channel broad launch事件Bounce Url
 *
 */

public class ChannelBounceUrlMR extends MRJob {

	@Override
	public boolean init(String date) {
        //输入列表
		inputList.add(DirConstant.WEBLOG_FilterLOG
				+ date);
        outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR +"ChannelBounceUrl/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
    	
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());
        
        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, ChannelBounceUrlMapper.class);
        
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ThreeStringWritable.class);
        

        //reducer
        job.setReducerClass(ChannelBounceUrlReducer.class);
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
    
    
    public static class ChannelBounceUrlMapper extends Mapper<NullWritable, Text, Text,ThreeStringWritable> {
   
             Text outKey = new Text();

		@Override
        public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
       	try {
    		       
       		        HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());
       	            String event = lineMap.get("event");
       	        	String url = lineMap.get("url");

    				NeteaseChannel_CS nce = NeteaseChannel_CS.getChannel(url);
				if (event.equals("launch")&&nce!=null){
					
					String sid = lineMap.get("sid");
				
					String pgr = lineMap.get("pgr");
					String str[] = lineMap.get("project").split("@version@");
		
					if (str.length==2){
						String	broad = str[1];
						if(broad.indexOf("_")==-1){
						ThreeStringWritable tsw =new ThreeStringWritable(broad,url,pgr);
						outKey.set(sid);
	                    context.write(outKey, tsw);
					}
					}
	
				}
            } catch (Exception e) {
				context.getCounter("ChannelBounceUrlMapper", "parseError").increment(1);
			}
        }

    }
    

       public static class ChannelBounceUrlReducer extends Reducer<Text, ThreeStringWritable, ThreeStringWritable, IntWritable> {
   		private IntWritable outputValue = new IntWritable(-1);
        
    	@Override
		protected void reduce(Text key, Iterable<ThreeStringWritable> values, Context context) throws IOException, InterruptedException {
		
            int i =0;
            ThreeStringWritable  temp =new ThreeStringWritable();
			for (ThreeStringWritable v : values) {
				i++;
	            if (i>1){
	            	break;
	            }
	            temp = v;
			}

		    if (i==1){
				NeteaseChannel_CS nce = NeteaseChannel_CS.getChannel(temp.getsecond());
   	    		if(NeteaseContentType.artical.match(temp.getsecond())){
   	    			ThreeStringWritable outKey = new ThreeStringWritable(nce.getName(),temp.getfirst(),temp.getsecond());
   				    context.write(outKey,outputValue);
   	    		}

		    }
		  
		}
    }
      

       
}





