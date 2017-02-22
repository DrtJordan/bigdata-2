package com.netease.weblogOffline.common;

import java.io.IOException;
import java.text.ParseException;

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
import com.netease.weblogOffline.data.ArticleWritable;
import com.netease.weblogOffline.data.LongArticleWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 更新Article全量文件
 * */
public class ArticleMergeMR extends MRJob {
	@Override
	public boolean init(String date) {
	    try {
            String theDayBefore = DateUtils.getTheDayBefore(date, 1);
            
            //输入列表
    		inputList.add(DirConstant.ARTICLE_INCR + date);
    		inputList.add(DirConstant.ARTICLE_ALL + theDayBefore);
    		
    		//输出列表
    		outputList.add(DirConstant.ARTICLE_ALL + date);
    		
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

    	MultipleInputs.addInputPath(job, new Path(inputList.get(0)), TextInputFormat.class, ArticleIncrMapper.class);
    	MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, ArticleAllMapper.class);
    	
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongArticleWritable.class);
        
        //reducer
        job.setReducerClass(MergeReducer.class);
        job.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArticleWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class ArticleIncrMapper extends Mapper<LongWritable, Text, Text, LongArticleWritable> {
    	
    	private Text outputKey = new Text();
    	private ArticleWritable aw=new ArticleWritable();
    	
    	private LongArticleWritable outputValue = new LongArticleWritable();
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
        		String line = value.toString();
        		//
        		
        		while(line.contains("\t\t")){
    				line = line.replace("\t\t", "\t(null)\t");
    			}
        		if (line.endsWith("\t")){
        			line= line+"(null)";
        		}
        		
        		String[] strs = line.split("\t");
        		if(strs.length == 15){
                    outputKey.set(strs[1]);
        		  	outputValue.setFirst(1); 
        		  	aw.setDocId(strs[0]);
        		  	aw.setArticleUrl(strs[1]);
        		  	aw.setCombineGentie(strs[2]);
        		  	aw.setChannel(strs[3]);
        		  	aw.setCloumn(strs[4]);
        		  	aw.setSource(strs[5]);
        		  	aw.setSourceUrl(strs[6]);
        		  	aw.setTitle(strs[7]);
        		  	aw.setKeyWord(strs[8]);
        		  	aw.setReporter(strs[9]);
        		  	aw.setSourceTag(strs[10]);
        		  	aw.setOriginalAuthor(strs[11]);
        		  	aw.setPublishTime(strs[12]);
        		  	aw.setEditor(strs[13]);
        		  	aw.setLmodify(strs[14]);
        		  	
        		  	outputValue.setSecond(aw);
        		  	context.write(outputKey, outputValue);
        		}else{
        			context.getCounter("PhotoSetIncrMapper", "parseLineError").increment(1);
        		}
			} catch (Exception e) {
				context.getCounter("PhotoSetIncrMapper", "mapException").increment(1);
			}
        }
    }
    
    public static class ArticleAllMapper extends Mapper<Text, ArticleWritable, Text, LongArticleWritable> {
    	private LongArticleWritable outputValue = new LongArticleWritable();
        @Override
        public void map(Text key, ArticleWritable value, Context context) throws IOException, InterruptedException {
        	outputValue.setFirst(0);
        	outputValue.setSecond(value);
        	context.write(key, outputValue);
        }
    }
    
	public static class MergeReducer extends Reducer<Text, LongArticleWritable, Text, ArticleWritable> {
		private ArticleWritable aw ;
		
        @Override
        protected void reduce(Text key, Iterable<LongArticleWritable> values, Context context) throws IOException, InterruptedException {
    		try {
    			aw = null;
            	for(LongArticleWritable lsw : values){
            		if((0 == lsw.getFirst() && null == aw) 
            				|| 1 == lsw.getFirst()){
            			aw = lsw.getSecond();
            		}
            	}
    		
    		
             context.write(key, aw);
			} catch (Exception e) {
				context.getCounter("MergeReducer", "mapException").increment(1);
			}
        }
	}
}












