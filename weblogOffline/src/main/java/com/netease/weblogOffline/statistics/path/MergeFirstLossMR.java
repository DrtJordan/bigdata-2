package com.netease.weblogOffline.statistics.path;

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
import com.netease.weblogOffline.data.StringStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 用户访问路径统计入口页流失
 * 1、统计入口页的流失（来自入口页-入口页跳到1级页面）
 *
 * 2、自入口页-入口页跳到1级页面 流失 计数
 *
 * 3、入口页跳到1级页面 流失 计数
 *
 * Created by hfchen on 2015/4/27.
 */
public class MergeFirstLossMR extends MRJob {


    @Override
    public boolean init(String date) {
        inputList.add(DirConstant.PATH_MIDLAYER_DIR + "pathentry/" + date);
        inputList.add(DirConstant.PATH_MIDLAYER_DIR + "pathnotentry/" + date);
        outputList.add(DirConstant.PATH_TEMP_DIR + "entryfirstlosecount/" + date);
        outputList.add(DirConstant.PATH_TEMP_DIR + "entryfirstlosecount1/" + date);
        outputList.add(DirConstant.PATH_STATISTICS_DIR + "entryfirstlosecount/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;

        //1\得到入口页面记录 后统计入口页面到一级页面的流失  <一级-from,1>
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");
        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, EntryMapper.class);
        MultipleInputs.addInputPath(job1, new Path(inputList.get(1)), SequenceFileInputFormat.class, NotEntryMapper.class);

        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(StringStringWritable.class);

        //reducer
        job1.setReducerClass(CountEntryOrNotReducer.class);
        job1.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setOutputKeyClass(StringStringWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }


        //2\统计入口页面到一级页面的流量<一级-from,count>
        Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-2");

        MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, Temp3Mapper.class);

        //mapper
        job2.setMapOutputKeyClass(StringStringWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);

        //reducer
        job2.setReducerClass(CountAllReducer.class);
        job2.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        //        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setCombinerClass(CountAllReducer.class);
        job2.setOutputKeyClass(StringStringWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }


        //3\统计流失<一级,int>
        Job job3 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-3");

        MultipleInputs.addInputPath(job3, new Path(outputList.get(1)), SequenceFileInputFormat.class, TranMapper.class);

        //mapper
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);

        //reducer
        job3.setReducerClass(CountTextAllReducer.class);
        job3.setCombinerClass(CountTextAllReducer.class);
        job3.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job3, new Path(outputList.get(2)));
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        if(!job3.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }

        return jobState;
    }

    public static class CountReducer extends Reducer<StringStringWritable, IntWritable, StringStringWritable, IntWritable> {

        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(StringStringWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable v : values){
                sum += v.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }


    /**
     * 去掉www或者com的后缀
     * @param URL
     * @return
     */
    private static String filterUrl(String URL){
        String filterurl = URL==null? null: URL.toLowerCase();
        if(filterurl!=null && filterurl.length()>0){
            if(filterurl.endsWith(".")){
                filterurl = filterurl.substring(0, filterurl.length()-1);
            }
            if(filterurl.contains(".cz")
                || filterurl.contains(".com")
                || filterurl.contains(".cn")
                || filterurl.contains(".org")
                || filterurl.contains(".edu")
                || filterurl.contains(".net")){

                filterurl = getFilterUrl(filterurl, ".cz");
                filterurl = getFilterUrl(filterurl, ".com");
                filterurl = getFilterUrl(filterurl, ".cn");
                filterurl = getFilterUrl(filterurl, ".org");
                filterurl = getFilterUrl(filterurl, ".edu");
                filterurl = getFilterUrl(filterurl, ".net");
                if(filterurl.contains(".")){
                    filterurl = filterurl.substring(filterurl.lastIndexOf(".")+1);
                }
            }
        }
        return filterurl;
    }

    private static String getFilterUrl(String filterurl, String rule) {
        if(filterurl.endsWith(rule)){
            filterurl = filterurl.substring(0, filterurl.lastIndexOf(rule));
        }
        return filterurl;
    }

    public static class EntryMapper extends Mapper<StringStringWritable, Text, Text, StringStringWritable> {
        @Override
        public void map(StringStringWritable key, Text value, Context context) throws IOException, InterruptedException {
            key.setFirst("entry"+key.getFirst());
            key.setSecond(filterUrl(key.getSecond()));
            context.write(value, key);
        }

    }

    public static class NotEntryMapper extends Mapper<StringStringWritable, Text, Text, StringStringWritable> {

        @Override
        public void map(StringStringWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }
    }


    public static class CountEntryOrNotReducer extends Reducer<Text, StringStringWritable, StringStringWritable, IntWritable> {

        private StringStringWritable outputKey = new StringStringWritable();
        private IntWritable outputValue = new IntWritable(1);
        boolean hasEnter = false;
        boolean hasFirst = false;

        @Override
        protected void reduce(Text key, Iterable<StringStringWritable> values, Context context) throws IOException, InterruptedException {
            hasEnter = false;
            hasFirst = false;
            for(StringStringWritable v : values){
                if(v.getFirst().toString().indexOf("entry")==0){
                    outputKey.setFirst(v.getFirst().substring(5));//first entry  频道文章图集
                    outputKey.setSecond(v.getSecond());// first entry domain 来自某个导航
                    hasEnter = true;
                } else if(v.getFirst().toString().indexOf("entry")!=0){
                    hasFirst = true;
                }
            }
            if(hasEnter && !hasFirst){
                context.write(outputKey, outputValue);
                if(outputKey.getFirst().toString().equals("163Home")){
                    context.getCounter("secondmr", "163Homelost").increment(1);
                }
            }
        }
    }




    public static class Temp3Mapper extends Mapper<StringStringWritable, IntWritable, StringStringWritable, IntWritable> {

        @Override
        public void map(StringStringWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }


    public static class CountAllReducer extends Reducer<StringStringWritable, IntWritable, StringStringWritable, IntWritable> {

        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(StringStringWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable v : values){
                sum += v.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }


    private static class TranMapper extends Mapper<StringStringWritable, IntWritable, Text, IntWritable> {

        private Text outputKey = new Text();

        @Override
        public void map(StringStringWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            outputKey.set(key.getFirst()); // first 图集等
            context.write(outputKey, value);
        }
    }


    public static class CountTextAllReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable v : values){
                sum += v.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }




}
