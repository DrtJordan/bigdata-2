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
 *
 * 1\统计来源到入口页相关数据
 * 2、过滤来源的url重新得到《入口页+来源，count》
 * Created by hfchen on 2015/4/27.
 */
public class SourceEnterVisitPathMR extends MRJob {


    @Override
    public boolean init(String date) {
        inputList.add(DirConstant.PATH_MIDLAYER_DIR + "pathentry/" + date);
        inputList.add(DirConstant.PATH_MIDLAYER_DIR + "pathnotentry/" + date);
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "entrycountpath/" + date);
        outputList.add(DirConstant.PATH_STATISTICS_DIR + "entrycountpath/" + date);//来源到入口页的流量
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;

        //1\得到入口页面记录,得到入口页面记录 后统计入口页面流量 <一级-from， count>
        Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");

        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, TempMapper.class);

        //mapper
        job.setMapOutputKeyClass(StringStringWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reducer
        job.setReducerClass(CountReducer.class);
        job.setCombinerClass(CountReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(StringStringWritable.class);
        job.setOutputValueClass(IntWritable.class);

        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }


        //2\得到入口页面记录,得到入口页面记录 后统计入口页面流量 <一级-from， count>
        Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-2");

        MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, TempFilterURLMapper.class);

        //mapper
        job2.setMapOutputKeyClass(StringStringWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);

        //reducer
        job2.setReducerClass(CountReducer.class);
        job2.setCombinerClass(CountReducer.class);
        job2.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        //        job.setOutputFormatClass(TextOutputFormat.class);

        job2.setOutputKeyClass(StringStringWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        return jobState;
    }








    public static class TempMapper extends Mapper<StringStringWritable, Text, StringStringWritable, IntWritable> {

        private IntWritable outputvalue = new IntWritable(1);

        @Override
        public void map(StringStringWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, outputvalue);
        }
    }


    /**
     * 过滤url
     */
    public static class TempFilterURLMapper extends Mapper<StringStringWritable, IntWritable, StringStringWritable, IntWritable> {

        private StringStringWritable outputKey = new StringStringWritable();

        @Override
        public void map(StringStringWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            outputKey.setFirst(key.getFirst());
            outputKey.setSecond(filterUrl(key.getSecond()));
//            if(value.get()<1000){
//                outputKey.setSecond("othersmallone");
//            }
            context.write(outputKey, value);
        }
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

}
