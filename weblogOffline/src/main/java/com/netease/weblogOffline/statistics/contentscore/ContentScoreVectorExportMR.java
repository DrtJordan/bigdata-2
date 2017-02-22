package com.netease.weblogOffline.statistics.contentscore;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
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
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.ContentScoreVector;
import com.netease.weblogOffline.data.YcInfo;
import com.netease.weblogOffline.utils.HadoopUtils;

public class ContentScoreVectorExportMR extends MRJob {
	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.WEBLOG_MIDLAYER_DIR + "contentScoreVector/" + date);
		inputList.add(DirConstant.YC_INFO_ALL + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_OTHER_DIR + "ycDetail/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());
    	
    	int days = 14;
		try {
			days = Integer.parseInt(getParams().getUserDefineParam("days", "30"));
		} catch (Exception e) {
			LOG.error(e);
		}
		String fromDate = DateUtils.getTheDayBefore(getParams().getDate(), days);
    	job.getConfiguration().setLong("fromTime", DateUtils.toLongTime(fromDate, "yyyyMMdd"));
    	job.getConfiguration().set("date", getParams().getDate());
    	
    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, ContentScoreVectorMapper.class);
    	MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, CmsYcInfoMapper.class);
		
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        //reducer
        job.setReducerClass(MergeReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class ContentScoreVectorMapper extends Mapper<Text, ContentScoreVector, Text, Text> {
    	
    	private Text outputValue = new Text();
    	
        @Override
        public void map(Text key, ContentScoreVector value, Context context) throws IOException, InterruptedException {
			String s = value.getUrl() + "\t" + value.getChannel() + "\t"
					+ value.getTitle() + "\t" + value.getLmodify() + "\t"
					+ value.getType() + "\t" + value.getAuthor() + "\t"
					+ value.getPv() + "\t" + value.getUv() + "\t" 
					+ value.getGenTieCount() + "\t" + value.getShareCount() + "\t" 
					+ value.getBackCount();
        	outputValue.set(s.toString());
        	context.write(key, outputValue);
        }
    }
    
    public static class CmsYcInfoMapper extends Mapper<Text, YcInfo, Text, Text> {
    	private Text outputValue = new Text();
    	private long fromTime = 0;
    	
    	protected void setup(Context context) throws IOException, InterruptedException {
    		fromTime = context.getConfiguration().getLong("fromTime", 0);
    	}
    	
        @Override
        public void map(Text key, YcInfo value, Context context) throws IOException, InterruptedException {
        	long lmodifyTime = DateUtils.toLongTime(value.getLmodify(), "yyyy-MM-dd HH:mm:ss");
        	if(lmodifyTime > fromTime){
        		
        		outputValue.set("output-" + value.getTitle() + "\t" +value.getLmodify() + "\t" + value.getAuthor());
        		context.write(key, outputValue);
        	}
        }
    }
    
    public static class MergeReducer extends Reducer<Text, Text, Text, Text> {
    	private long dateTime = 0;
    	
    	protected void setup(Context context) throws IOException, InterruptedException {
    		String date = context.getConfiguration().get("date");
    		dateTime = DateUtils.toLongTime(date, "yyyyMMdd");
    	}
    	
    	private Text outputKey = new Text();
    	private Text outputValue = new Text("");
    	
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	String url = key.toString();
        	
        	String ycInfo = null;
        	String line = null;
        	for (Text value : values) {
        		String val = value.toString();
        		if(val.startsWith("output-")){
        			ycInfo = val.substring(7, val.length());
        		}else{
        			line = value.toString();
        		}
        	}
        	
        	if(null != ycInfo){
        		if(line == null){
            		line = url + "\t" + NeteaseChannel_CS.getChannelName(url) + "\t" + ycInfo + "\t" 
            				+ NeteaseContentType.getTypeName(url) + "\t0\t0\t0\t0";
            	}
            	
//        		long lmodifyTime = DateUtils.toLongTime(line.split("\t")[3], "yyyy-MM-dd HH:mm:ss");
        		long lmodifyTime = DateUtils.toLongTime(line.split("\t")[3].substring(0, 10), "yyyy-MM-dd");
            	
        		
        		if(dateTime > 0){
        			long dif = (dateTime - lmodifyTime)/(1000 * 60 * 60 * 24) + 1;
        	        
                	outputKey.set(line + "\t" + dif);
            		context.write(outputKey, outputValue);
        		}
            	
        	}
        }
    }
    
}












