package com.netease.weblogOffline.common;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;

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
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogOffline.data.ColumnOFContent_3gWritable;
import com.netease.weblogOffline.data.Content_3gWritable;
import com.netease.weblogOffline.data.LongContent_3gWritable;
import com.netease.weblogOffline.data.LongStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;


/**
 * 更新Content_3g全量文件
 * */
public class Content_3gMergeMR_new extends MRJob {
	@Override
	public boolean init(String date) {
	    try {
            String theDayBefore = DateUtils.getTheDayBefore(date, 1);
            
            //输入列表
    		inputList.add(DirConstant.LOG_OF_3G_INCR + date);
    		inputList.add(DirConstant.LOG_OF_3G_ALL + theDayBefore);
    		
    		inputList.add(DirConstant.LOG_OF_3G_EDITOR + date);
    		
    		//输出列表
    		outputList.add(DirConstant.LOG_OF_3G_ALL + date);
    		
            return true;
        } catch (ParseException e) {
            LOG.error(e);
            return false;
        }
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());

    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), TextInputFormat.class, Content_3gIncrMapper.class);
    	MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, Content_3gAllMapper.class);
    	MultipleInputs.addInputPath(job, new Path(inputList.get(2)), TextInputFormat.class, Content_3gEditorMapper.class);
    	String dt =this.getParams().getDate();
    	job.getConfiguration().set("dt", dt);
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongContent_3gWritable.class);
        
        //reducer
        job.setReducerClass(MergeReducer.class);
        job.setNumReduceTasks(64);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Content_3gWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class Content_3gIncrMapper extends Mapper<LongWritable, Text, Text, LongContent_3gWritable> {
    	
    	private Text outputKey = new Text();

    	
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
        		LongContent_3gWritable outputValue = new LongContent_3gWritable();
        		String line = value.toString();
        		//栏目名称	栏目id	docid	图集id(或视频id、专题id、空)	类型（文章/图集/视频/专题）	标题 	3w发布器url	3g发布器url
        		
        		while(line.contains("\t\t")){
    				line = line.replace("\t\t", "\t(null)\t");
    			}
        		if (line.endsWith("\t")){
        			line= line+"(null)";
        		}
        		
        		String[] strs = line.split("\t");
        		if(strs.length ==8){
        			ArrayList<ColumnOFContent_3gWritable> al = new ArrayList<ColumnOFContent_3gWritable>();
        			String docid = strs[2];
        			outputKey.set(docid);
        		  	outputValue.setFirst(1);
        		  	
        		  	ColumnOFContent_3gWritable  fcoc = new ColumnOFContent_3gWritable(strs[0],strs[1],strs[3],strs[4],strs[5],context.getConfiguration().get("dt"),"(null)","(null)");

        		  	al.add(fcoc);
        		  	Content_3gWritable c3g= new Content_3gWritable(strs[2],strs[6],strs[7],"(null)", al);
        		  	outputValue.setSecond(c3g);
        		  	context.write(outputKey, outputValue);
        		}else if (strs.length ==9){
        			ArrayList<ColumnOFContent_3gWritable> al = new ArrayList<ColumnOFContent_3gWritable>();
        			String docid = strs[2];
        			outputKey.set(docid);
        		  	outputValue.setFirst(1);
        		  	
        		  	ColumnOFContent_3gWritable  fcoc = new ColumnOFContent_3gWritable(strs[0],strs[1],strs[3],strs[4],strs[5],context.getConfiguration().get("dt"),strs[8],"(null)");

        		  	al.add(fcoc);
        		  	Content_3gWritable c3g= new Content_3gWritable(strs[2],strs[6],strs[7],"(null)", al);
        		  	outputValue.setSecond(c3g);
        		  	context.write(outputKey, outputValue);
        		}else if(strs.length ==10){
        			ArrayList<ColumnOFContent_3gWritable> al = new ArrayList<ColumnOFContent_3gWritable>();
        			String docid = strs[2];
        			outputKey.set(docid);
        		  	outputValue.setFirst(1);
        		  	
        		  	ColumnOFContent_3gWritable  fcoc = new ColumnOFContent_3gWritable(strs[0],strs[1],strs[3],strs[4],strs[5],context.getConfiguration().get("dt"),strs[8],"(null)");

        		  	al.add(fcoc);
        		  	Content_3gWritable c3g= new Content_3gWritable(strs[2],strs[6],strs[7],strs[9], al);
        		  	outputValue.setSecond(c3g);
        		  	context.write(outputKey, outputValue);
        		}else if (strs.length ==11){
        			ArrayList<ColumnOFContent_3gWritable> al = new ArrayList<ColumnOFContent_3gWritable>();
        			String docid = strs[2];
        			outputKey.set(docid);
        		  	outputValue.setFirst(1);
        		  	
        		  	ColumnOFContent_3gWritable  fcoc = new ColumnOFContent_3gWritable(strs[0],strs[1],strs[3],strs[4],strs[5],context.getConfiguration().get("dt"),strs[8],"(null)");

        		  	al.add(fcoc);
        		  	Content_3gWritable c3g= new Content_3gWritable(strs[2],strs[6],strs[7],strs[9], al);
        		  	outputValue.setSecond(c3g);
        		  	context.write(outputKey, outputValue);
        		}else if(strs.length ==12){
        			ArrayList<ColumnOFContent_3gWritable> al = new ArrayList<ColumnOFContent_3gWritable>();
        			String docid = strs[2];
        			outputKey.set(docid);
        		  	outputValue.setFirst(1);
        		  	
        		  	ColumnOFContent_3gWritable  fcoc = new ColumnOFContent_3gWritable(strs[0],strs[1],strs[3],strs[4],strs[5],context.getConfiguration().get("dt"),strs[8],strs[11]);

        		  	al.add(fcoc);
        		  	Content_3gWritable c3g= new Content_3gWritable(strs[2],strs[6],strs[7],strs[9], al);
        		  	outputValue.setSecond(c3g);
        		  	context.write(outputKey, outputValue);
        		}else{
        		
        			context.getCounter("Content_3gIncrMapper", "parseLineError").increment(1);
        		}
			} catch (Exception e) {
				context.getCounter("Content_3gIncrMapper", "mapException").increment(1);
			}
        }
    }
    
    
 public static class Content_3gEditorMapper extends Mapper<LongWritable, Text, Text, LongContent_3gWritable> {
    	
    	private Text outputKey = new Text();

    	
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
        		LongContent_3gWritable outputValue = new LongContent_3gWritable();
        		String line = value.toString();
        		//docid	editor lmodify
        		
        		while(line.contains("\t\t")){
    				line = line.replace("\t\t", "\t(null)\t");
    			}
        		if (line.endsWith("\t")){
        			line= line+"(null)";
        		}
        		
        		String[] strs = line.split("\t");
        		if(strs.length >=3){

        			String docid = strs[0];
        			outputKey.set(docid);
        		  	outputValue.setFirst(2);
        		  	ArrayList<ColumnOFContent_3gWritable> al = new ArrayList<ColumnOFContent_3gWritable>();
        		  	ColumnOFContent_3gWritable  fcoc = new ColumnOFContent_3gWritable("","","","","","","","");

        		  	al.add(fcoc);
        		  	outputValue.setSecond(new Content_3gWritable("","","",strs[1], al));
        		  	context.write(outputKey, outputValue);
        		}else{
        		
        			context.getCounter("Content_3gIncrMapper", "parseLineError").increment(1);
        		}
			} catch (Exception e) {
				context.getCounter("Content_3gIncrMapper", "mapException").increment(1);
			}
        }
    }
    public static class Content_3gAllMapper extends Mapper<Text, Content_3gWritable, Text, LongContent_3gWritable> {
    	private LongContent_3gWritable outputValue = new LongContent_3gWritable();
        @Override
        public void map(Text key, Content_3gWritable value, Context context) throws IOException, InterruptedException {
        	outputValue.setFirst(0);
        	outputValue.setSecond(value);
        	context.write(key, outputValue);
        }
    }
    
    
    
    
	public static class MergeReducer extends Reducer<Text, LongContent_3gWritable, Text, Content_3gWritable> {
		
		   ArrayList<Content_3gWritable> al_new = new ArrayList<Content_3gWritable>();
		   HashSet<ColumnOFContent_3gWritable> hs0 = new HashSet<ColumnOFContent_3gWritable>();	
	
        @Override
        protected void reduce(Text key, Iterable<LongContent_3gWritable> values, Context context) throws IOException, InterruptedException {
    		try {
    			al_new.clear();
    			hs0.clear();
    			Content_3gWritable old = null;   	
    			Content_3gWritable now = null;
    			Content_3gWritable editor = null;
        	for(LongContent_3gWritable lsw : values){
	
        		if (lsw.getFirst()==0){
        			old = new Content_3gWritable(lsw.getSecond());
        		}else if (lsw.getFirst()==1){
        			al_new.add(new Content_3gWritable(lsw.getSecond()));
        		}else if (lsw.getFirst()==2){
        			editor = new Content_3gWritable(lsw.getSecond());
        		}

        	}
        	if (old ==null){
        		now = new Content_3gWritable(al_new.get(0));
        		
        	}else {
        		now = new Content_3gWritable(old);
        		hs0.addAll(old.getContent_3g_list());
        	}
        	
        	for (Content_3gWritable c:al_new){
    			hs0.addAll(c.getContent_3g_list());
        	}
        	now.getContent_3g_list().clear();
        	for (ColumnOFContent_3gWritable cc:hs0){
        		now.getContent_3g_list().add(new ColumnOFContent_3gWritable(cc));
        	}
        	if(editor!=null){
        		now.setEditor(editor.getEditor());
        	}
            context.write(key, now);
			} catch (Exception e) {
				context.getCounter("MergeReducer", "mapException").increment(1);
			}
        }
	}
}












