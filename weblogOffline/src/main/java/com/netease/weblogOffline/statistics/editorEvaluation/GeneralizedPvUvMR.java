package com.netease.weblogOffline.statistics.editorEvaluation;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.annotation.NotCheckInputFile;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogOffline.data.MultiStatisticResultWritable;
import com.netease.weblogOffline.data.StatisticResultWritable;
import com.netease.weblogOffline.data.StringStringWritable;
import com.netease.weblogOffline.data.StringTriWritable;
import com.netease.weblogOffline.statistics.editorEvaluation.data.PvUvArgs;
import com.netease.weblogOffline.statistics.editorEvaluation.generators.PvUvArgsGenerator;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 通用pv、uv计算。
 * 
 * 执行：
 * sh runMRJob.sh -c com.netease.weblogOffline.statistics.contentStatistics.GeneralizedPvUvMR -d $yesterday 
 * 	-input 输入数据目录 -output 中间数据存储目录1，中间数据存储目录2，结果数据存储目录 --pvUvArgsGenerator com.netease.weblogOffline.statistics.contentStatistics.PvUvArgsGeneratorApp
 * 
 * 
 * 
 * */
public class GeneralizedPvUvMR extends MRJob {
	
	@Override
	public boolean init(String date) {
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
    	
        /******************************************************/
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");
    	job1.getConfiguration().set("pvUvArgsGenerator", getUserDefineParam("pvUvArgsGenerator", ""));
    	boolean b1 =false;
    	b1|=addInputPath(job1, new Path(inputList.get(0)), TextInputFormat.class, LogMapper.class);
    	if (!b1){
			return jobState;
		}
        //mapper
        job1.setMapOutputKeyClass(StringTriWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);
        
        job1.setCombinerClass(PvUvTempReducer.class);
        
        //reducer
        job1.setReducerClass(PvUvTempReducer.class);
        job1.setNumReduceTasks(32);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(StringTriWritable.class);
        job1.setOutputValueClass(LongWritable.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        
        /******************************************************/
    	Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step2");
    	boolean b2 =false;
    	b2|=addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, PvUvMapper.class);
    	if (!b2){
			return jobState;
		}
        //mapper
        job2.setMapOutputKeyClass(StringStringWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);
        
        //reducer
        job2.setReducerClass(PvUvReducer.class);
        job2.setNumReduceTasks(32);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job2.setOutputKeyClass(StringStringWritable.class);
        job2.setOutputValueClass(StatisticResultWritable.class);
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        
        /******************************************************/
        Job job3 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step3");
    	boolean b3 =false;
    	b3|=addInputPath(job3, new Path(outputList.get(1)), SequenceFileInputFormat.class, FormatMapper.class);
    	if (!b3){
			return jobState;
		}
        //mapper
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(MultiStatisticResultWritable.class);
        
        //reducer
        job3.setReducerClass(FormatReducer.class);
        job3.setNumReduceTasks(32);
        FileOutputFormat.setOutputPath(job3, new Path(outputList.get(2)));
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(MultiStatisticResultWritable.class);
        
        if(!job3.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class LogMapper extends Mapper<LongWritable, Text, StringTriWritable, LongWritable> {
    	
    	private PvUvArgsGenerator argsGenerator; 
    	
    	private StringTriWritable outputKey = new StringTriWritable();
    	private LongWritable outputValue = new LongWritable();
    	
    	protected void setup(Context context) throws IOException, InterruptedException {
    			String urlUidParserClass = context.getConfiguration().get("pvUvArgsGenerator");
    			if(StringUtils.isNotBlank(urlUidParserClass)){
    				try {
    					argsGenerator  = (PvUvArgsGenerator) Class.forName(urlUidParserClass).newInstance();
					} catch (Exception e) {
						context.getCounter("LogMapper", "setupException").increment(1);
					}
    			}
    	}
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
        		List<PvUvArgs> args = argsGenerator.execute(value.toString());
        		
        		if(args == null || args.size() == 0){
        			context.getCounter("LogMapper", "nullArgs").increment(1);
        			return;
        		}
        		System.out.println("args=" + args);
        		for(PvUvArgs obj : args){
        			outputKey.setFirst(obj.getKey());
        			outputKey.setSecond(obj.getGroup());
        			outputKey.setThird(obj.getUid());
        			outputValue.set(obj.getCount());
	        		context.write(outputKey, outputValue);
        		}
			} catch (Exception e) {
				e.printStackTrace();
				context.getCounter("LogMapper", "mapException").increment(1);
			}
        }
    }
    
    public static class PvUvTempReducer extends Reducer<StringTriWritable, LongWritable, StringTriWritable, LongWritable> {
    	
    	private LongWritable outputValue = new LongWritable();
    	
        @Override
        protected void reduce(StringTriWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        	long sum = 0;
        	
        	for(LongWritable val : values){
        		sum += val.get();
        	}
        	
        	outputValue.set(sum);
			context.write(key, outputValue);
        }
    }
    
    public static class PvUvMapper extends Mapper<StringTriWritable, LongWritable, StringStringWritable, LongWritable> {
    	private StringStringWritable outputKey = new StringStringWritable();
    	
        @Override
        public void map(StringTriWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
        	outputKey.setFirst(key.getFirst());
        	outputKey.setSecond(key.getSecond());
        	context.write(outputKey, value);
        }
    }
    
    public static class PvUvReducer extends Reducer<StringStringWritable, LongWritable, StringStringWritable, StatisticResultWritable> {
    	
    	private StatisticResultWritable outputValue = new StatisticResultWritable();
    	
        @Override
        protected void reduce(StringStringWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        	int pv = 0;
        	int uv = 0;
        	
        	for(LongWritable val : values){
        		pv += val.get();
        		uv++;
        	}
        	
        	outputValue.setPv(pv);
        	outputValue.setUv(uv);
			context.write(key, outputValue);
        }
    }
    
    public static class FormatMapper extends Mapper<StringStringWritable, StatisticResultWritable, Text, MultiStatisticResultWritable> {
    	private Text outputKey = new Text();
    	private MultiStatisticResultWritable outputValue = new MultiStatisticResultWritable();
    	
        @Override
        public void map(StringStringWritable key, StatisticResultWritable value, Context context) throws IOException, InterruptedException {
        	outputValue.getDataMap().clear();
        	
        	String k = key.getFirst();
        	String group = key.getSecond();
        	
        	outputKey.set(k);
        	outputValue.getDataMap().put(new Text(group), value);
        	
        	context.write(outputKey, outputValue);
        }
    }
    
    public static class FormatReducer extends Reducer<Text, MultiStatisticResultWritable, Text, MultiStatisticResultWritable> {
    	
    	private MultiStatisticResultWritable outputValue = new MultiStatisticResultWritable();
    	
        @Override
        protected void reduce(Text key, Iterable<MultiStatisticResultWritable> values, Context context) throws IOException, InterruptedException {
        	outputValue.getDataMap().clear();
        	
        	for(MultiStatisticResultWritable val : values){
        		outputValue.add(val);
        	}
        	
			context.write(key, outputValue);
        }
    }
}












