package com.netease.weblogOffline.statistics.sessionpath;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.logparsers.WeblogParser;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 用户访问入口（入口页和非入口页）
 *
 * Created by ruihuang on 2015/5/26.
 */
public class WeblogEnterMR extends MRJob {


    @Override
    public boolean init(String date) {
        inputList.add(DirConstant.WEBLOG_LOG + date);
        outputList.add(DirConstant.PATH_MIDLAYER_DIR + "sessionenterornot/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;

        //1\得到入口页面记录WeblogEnterMR
        Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());

//        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), TextInputFormat.class, LogMapper.class);
        for(int i=0;i<inputList.size();i++){
            MultipleInputs.addInputPath(job, new Path(inputList.get(i)), TextInputFormat.class, LogMapper.class);
        }

        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //reducer
        job.setReducerClass(EntryOrNotReducer.class);
        job.setNumReduceTasks(100);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
//        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        return jobState;
    }

    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outputkey = new Text();
        private Text outputvalue = new Text();

        private LogParser logParser = new WeblogParser();

        String event = null;
        String entry = null;
        String url = null;
        String ref = null;
        String pgr = null;
        String prev_pgr = null;
        String serverTime = null;
        String sid = null;
        String tempOutputvalue = null;


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Map<String, String> logMap = logParser.parse(value.toString());
                event = logMap.get("event");
                entry = logMap.get("entry");
                pgr = logMap.get("pgr");
                prev_pgr = logMap.get("prev_pgr");
                serverTime = logMap.get("serverTime");
                sid = logMap.get("sid");

                //获得入口页的分类，ref（来源）,当前页面随机数，访问时间
                if("launch".equals(event) && "1".equals(entry)){
                    url = logMap.get("url");
                    ref = logMap.get("ref");

                    String refDomain = UrlUtils.urlGetDomain(logMap.get("ref"));
                    if(refDomain.length()==0){
                        refDomain = "(empty)";
                    }
                    
                    tempOutputvalue = getCategoryByURL(url) + "," + refDomain + "," + pgr + "," + serverTime ;
                    
                    outputkey.set(sid);
                    outputvalue.set(tempOutputvalue+"entry");
                    //outputkey.setFirst(getCategoryByURL(url));
                    //outputkey.setSecond(refDomain);
                    //outputValue.set(pgr+"entry");
                    context.write(outputkey, outputvalue);

                } else if("launch".equals(event) && !"1".equals(entry) && prev_pgr!=null && prev_pgr.length()>0){
                    //获得非入口页的匪类，来源页面的随机数，当前页面随机数，访问时间
                    url = logMap.get("url");
                    
                    tempOutputvalue = getCategoryByURL(url) + "," + prev_pgr + "," + pgr + "," + serverTime;
                    outputkey.set(sid);
                    outputvalue.set(tempOutputvalue);
                    //outputkey.setFirst(getCategoryByURL(url));
                    //outputkey.setSecond(pgr);
                    //outputValue.set(prev_pgr);
                    context.write(outputkey, outputvalue);

                }
            } catch (Exception e) {
                context.getCounter("LogMapper", "parseError").increment(1);
            }
        }
    }

    private static String getCategoryByURL(String url){
        if(UrlUtils.is163Home(url)){
            return "163Home"; //网首
        } else {
        	NeteaseChannel_CS nce = NeteaseChannel_CS.getChannel(url);
            if (nce != null) {
                if(nce.isHome(url)) {//频道首页
                    return nce.getName() + "Home";
                } else {//频道其它页
                    return nce.getName() + NeteaseContentType.getTypeName(url); //频道文章页、图集、专题页
                }
            }
        }
        return "other";
    }



    public static class EntryOrNotReducer extends Reducer<Text, Text, Text, Text> {

        private Text outputKey = new Text();
        String from = null;
 
        private MultipleOutputs<Text, Text> outputs = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outputs = new MultipleOutputs<Text, Text>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text v : values){
                if(v.toString().contains("entry")){
                    v.set(v.toString().replace("entry", ""));
                    outputs.write(key, v, "entry");
                } else {
                    outputs.write(key, v, "notentry");
                }
            }
        }
    }




}
