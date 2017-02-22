package com.netease.weblogOffline.common;

import java.io.IOException;

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
import com.netease.weblogOffline.data.ArticleWritable;
import com.netease.weblogOffline.utils.HadoopUtils;



public class DocidAndUrlOfArticleMR extends MRJob {

    @Override
    public boolean init(String date) {

        inputList.add(DirConstant.ARTICLE_ALL + date);
        outputList.add(DirConstant.COMMON_DATA_DIR + "docidAndUrlOfRarticle/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");
        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, ArticleMapper.class);
       
        // mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        // reducer
        job1.setReducerClass(DocidAndUrlOfRarticleReducer.class);
        job1.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        if (!job1.waitForCompletion(true)) {
            jobState = JobConstant.FAILED;
        }

        return jobState;
    }
    
    
    public static class ArticleMapper extends Mapper<Text, ArticleWritable, Text, Text> {
		private Text  outValue = new Text();
		private Text  outKey = new Text();
		@Override
		public void map(Text key, ArticleWritable value, Context context) throws IOException, InterruptedException {
            
			outKey.set(value.getDocId());
			
			
			outValue.set(value.getArticleUrl());
			context.write(outKey, outValue);//<docid,url>
		}
	}

 

    public static class DocidAndUrlOfRarticleReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
   
            for (Text val : values) {
            	context.write(key, val);//<docid,url>
     
            }
        }
    }

}
