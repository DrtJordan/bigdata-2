package com.netease.weblogOffline.statistics.cms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.ContentScoreVector;
import com.netease.weblogOffline.utils.HadoopUtils;

public class ChannelTieShareBackMR extends MRJob {

    @Override
    public boolean init(String date) {
        inputList.add(DirConstant.WEBLOG_MIDLAYER_DIR + "contentScoreVector/" + date);
        outputList.add(DirConstant.WEBLOG_STATISTICS_TODC_DIR + "cmstieshareback/" + date);
        return true;
    }
    
    @Override
    public int run(String[] arg0) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;
        
        Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());
        
        DistributedCache.addCacheFile(new URI(getHDFS().getUri().toString() +"/ntes_weblog/weblog/cache/cmsdata/domainchannel.txt"+"#domainchannel"), job.getConfiguration());
        
        //跟帖、分享、回流
        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, LogMapper.class);
        
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setCombinerClass(CountReducer.class);
        
        //reducer
        job.setReducerClass(CountReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        return jobState;
    }

    public static class LogMapper extends Mapper<Text, ContentScoreVector, Text, IntWritable>{
        
        Map<String, String> domainChannelMap = new HashMap<String, String>();
        Set<String> domainUrls = new HashSet<String>();
        
        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();
        
        @Override
        protected void setup(Mapper<Text, ContentScoreVector, Text, IntWritable>.Context context) throws IOException,
                InterruptedException {
            loadDomainChannelFile(context);
        }

        private void loadDomainChannelFile(Context context) throws IOException {
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(new File("domainchannel"))));
            String line = null;
            while ((line = br.readLine()) != null) {
                try {
                    String[] keyvalue = line.split("\t");
                    domainChannelMap.put(keyvalue[0], keyvalue[1]);
                } catch (Exception e) {
                    context.getCounter("LogMapper", "loadDomainChannelFile").increment(1);
                }
            }
            if (domainChannelMap != null) {
                domainUrls = domainChannelMap.keySet();
            }
            br.close();
        }
        
        @Override
        protected void map(Text key, ContentScoreVector value, Context context) throws IOException,
                InterruptedException {
            String url = value.getUrl();
            try {
                for(String domainUrl : domainUrls){
                    if(url.startsWith(domainUrl)){ //判断URL以哪个域开头，就算到对应频道
                        String channelId = domainChannelMap.get(domainUrl);
                        // 跟帖数
                        outputKey.set(channelId + "_genTieCount");
                        outputValue.set(value.getGenTieCount());
                        context.write(outputKey, outputValue);
                        // 分享数
                        outputKey.set(channelId + "_shareCount");
                        outputValue.set(value.getShareCount());
                        context.write(outputKey, outputValue);
                        // 回流数
                        outputKey.set(channelId + "_backCount");
                        outputValue.set(value.getBackCount());
                        context.write(outputKey, outputValue);
                    }
                }
                
            } catch (Exception e) {
                context.getCounter("LogMapper", "mapException").increment(1);
            }

        }

    }
    
    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable outputValue = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }

}
