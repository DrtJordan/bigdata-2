package com.netease.weblogOffline.statistics.dailyWeblog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.weblogfilter.WeblogFilterUtils;
import com.netease.weblogOffline.utils.HadoopUtils;

public class BounceRateMR extends MRJob {
	@Override
	public boolean init(String date) {
		//inputList.add(DirConstant.WEBLOG_LOG + date);
		inputList.add(DirConstant.WEBLOG_FilterLOG
				+ date);
	    outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "bounceRateTemp/" + date);
	    outputList.add(DirConstant.WEBLOG_STATISTICS_OTHER_DIR + "bounceRate/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");
        
        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, LogMapper.class);
        
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        
        //reducer
        job1.setReducerClass(BounceRateReducer.class);
        job1.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-2");
        job2.setJobName("BounceRateMR-step2");
        job2.setJarByClass(CounterMapper.class);
        job2.getConfiguration().set("mapred.job.queue.name", "weblog");
        
        MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, CounterMapper.class);
        
        //mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        
        job2.setCombinerClass(CounterReducer.class);
        
        //reducer
        job2.setReducerClass(CounterReducer.class);
        job2.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
        
        
//        else{
//        	String resultPath = getParams().getUserDefineParam("resultPath", "");
//        	if(StringUtils.isNotBlank(resultPath)){
//        		File resultFile = new File(resultPath);
//            	BufferedWriter w = new BufferedWriter(new FileWriter(resultFile));
//            	Iterator<Counter> iterator = job2.getCounters().getGroup("CountReducer").iterator();
//            	while(iterator.hasNext()){
//            		Counter counter = iterator.next();
//            		w.write(counter.getName() + " " + counter.getValue());
//            		w.newLine();
//            	}
//            	w.flush();w.close();
//        	}
//        }
        
    }
    
    public static class LogMapper extends Mapper<NullWritable, Text, Text, Text> {
    	
    	private Text outputkey = new Text();
    	private Text outputValue = new Text();
    	private Map<String, String> map = new HashMap<String, String>();
    	
        @Override
        public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        	map.clear();
        	try {

				HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());	
        	    
			     String event = lineMap.get("event");
				if("launch".equals(event)){
					String sid = lineMap.get("sid");
					String url = lineMap.get("url");
					String entry = lineMap.get("entry");
					String pver = lineMap.get("pver");
					String ptype = lineMap.get("ptype");
		
					map.put("url", url);
					map.put("pver", pver);
					map.put("ptype", ptype);
					if(null != entry){
						map.put("entry", entry);
					}
					
					outputkey.set(sid);
					outputValue.set(JsonUtils.toJson(map));
					context.write(outputkey, outputValue);
				}
			} catch (Exception e) {
				context.getCounter("LogMapper", "parseError").increment(1);
			}
        }
    }
    
    public static class BounceRateReducer extends Reducer<Text, Text, Text, IntWritable> {
    	
    	private Text outputKey = new Text();
    	private IntWritable outputValue = new IntWritable(1);
    	
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	boolean a = true;//网首跳出
        	boolean b = false;//session 包含网首
        	boolean c = false;//网首着陆
        	String pver = null;
        	String ptype = null;
        	
        	for(Text value : values){
        		Map<String, String> map = JsonUtils.json2Map(value.toString());
        		boolean isHome = UrlUtils.is163Home(map.get("url"));
        		if(null == pver && null != map.get("pver")){
        			pver = map.get("pver");
        		}
        		if(null == ptype && null != map.get("ptype")){
        			ptype = map.get("ptype");
        		}
        		if(!isHome){
        			a = false;
        		}else{
        			b = true;
        			if("1".equals(map.get("entry"))){
            			c = true;
            		}
        		}
        	}
        	
        	if(a){
//        		context.getCounter("CountReducer", "out163Home").increment(1);
//        		context.getCounter("CountReducer", "out163Home_" + pver + "_" + ptype).increment(1);
        		outputKey.set("out163Home");
        		context.write(outputKey, outputValue);
        		outputKey.set("out163Home_" + pver + "_" + ptype);
        		context.write(outputKey, outputValue);
        	}
        	
        	if(b){
//        		context.getCounter("CountReducer", "contain163Home").increment(1);
//        		context.getCounter("CountReducer", "contain163Home_" + pver + "_" + ptype).increment(1);
        		outputKey.set("contain163Home");
        		context.write(outputKey, outputValue);
        		outputKey.set("contain163Home_" + pver + "_" + ptype);
        		context.write(outputKey, outputValue);
        	}
        	
        	if(c){
//        		context.getCounter("CountReducer", "landing163Home").increment(1);
//        		context.getCounter("CountReducer", "landing163Home_" + pver + "_" + ptype).increment(1);
        		outputKey.set("landing163Home");
        		context.write(outputKey, outputValue);
        		outputKey.set("landing163Home_" + pver + "_" + ptype);
        		context.write(outputKey, outputValue);
        	}
        }
    }
    public static class CounterMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        @Override
        public void map(Text key, IntWritable value , Context context) throws IOException, InterruptedException {
        	context.write(key, value);
        }
    }
    
    public static class CounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
    	private IntWritable outputValue = new IntWritable();
    	
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum = 0;
        	for(IntWritable value : values){
        		sum += value.get();
        	}
        	outputValue.set(sum);
        	context.write(key, outputValue);
        }
    }
}












