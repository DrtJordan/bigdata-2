package com.netease.weblogOffline.statistics.media;

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
import com.netease.weblogCommon.data.enums.MediaUrlRegexChannel;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.ContentScoreVector;
import com.netease.weblogOffline.data.MediaScoreVector;
import com.netease.weblogOffline.data.StringStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 整合发布系统（url，子媒公司  全量）和大数据url指标属性（pv、uv、跟帖等属性）
 * 1、url子媒pvuv属性
 * 2、子媒频道总url个数、总pvuv
 * Created by hfchen on 2015/3/26.
 */
public class MediaMergeMR extends MRJob {


    @Override
    public boolean init(String date) {
        //输入列表
        inputList.add(DirConstant.URL_MEDIA_ALL + date);//media all new
        inputList.add(DirConstant.WEBLOG_MIDLAYER_DIR  + "contentScoreVector/" + date);//weblog url
        //输出列表
        outputList.add(DirConstant.MEDIA_MIDLAYER_DIR + "mediamerge/" + date);
        outputList.add(DirConstant.MEDIA_STATISTICS_DIR + "mediamerge/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;

        //合并子媒与url的信息
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");

        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, SubmediaMapper.class);
        MultipleInputs.addInputPath(job1, new Path(inputList.get(1)), SequenceFileInputFormat.class, ContentScoreVectorMapper.class);

        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(MediaScoreVector.class);

        //reducer
//        job1.setReducerClass(MergeReducer2.class);
        job1.setReducerClass(MergeReducer.class);
        job1.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(MediaScoreVector.class);

        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }


        //合并子媒与channel的信息
        Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step2");

        MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, ChannelMapper.class);

        //mapper
        job2.setMapOutputKeyClass(StringStringWritable.class);
        job2.setMapOutputValueClass(MediaScoreVector.class);

        //reducer
        job2.setReducerClass(CountReducer.class);
        job2.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        job2.setOutputKeyClass(StringStringWritable.class);
        job2.setOutputValueClass(MediaScoreVector.class);

        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }



//        //合并公司信息（根据子媒锁定公司）
//        Job job3 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step3");
//
//        MultipleInputs.addInputPath(job3, new Path(inputList.get(2)), TextInputFormat.class, CompanyMapper.class);
//
//        MultipleInputs.addInputPath(job3, new Path(outputList.get(1)), SequenceFileInputFormat.class, ResultMapper.class);
//        //mapper
//        job3.setMapOutputKeyClass(Text.class);
//        job3.setMapOutputValueClass(MediaScoreVector.class);
//
//        //reducer
//        //        job1.setReducerClass(MergeReducer2.class);
//        job3.setReducerClass(ResultReducer.class);
//        job3.setNumReduceTasks(1);
//        FileOutputFormat.setOutputPath(job3, new Path(outputList.get(2)));
//        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//        job3.setOutputKeyClass(StringTriWritable.class);
//        job3.setOutputValueClass(MediaScoreVector.class);
//
//        if(!job3.waitForCompletion(true)){
//            jobState = JobConstant.FAILED;
//        }


        return jobState;
    }

    public static class SubmediaMapper extends Mapper<Text, Text, Text, MediaScoreVector> {

        private MediaScoreVector outputValue = new MediaScoreVector();


        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            outputValue.setSubMedia(value.toString());
            context.write(key, outputValue);
        }
    }

    public static class ContentScoreVectorMapper extends Mapper<Text, ContentScoreVector, Text, MediaScoreVector> {

        private MediaScoreVector outputValue = null;

        @Override
        public void map(Text key, ContentScoreVector value, Context context) throws IOException, InterruptedException {
            outputValue = new MediaScoreVector(value);
            context.write(key, outputValue);
        }
    }


    public static class MergeReducer extends Reducer<Text, MediaScoreVector, Text, MediaScoreVector> {

        private String submeida = null;
        private Text outputKey = new Text();
        private MediaScoreVector outputValue = null;

        @Override
        protected void reduce(Text key, Iterable<MediaScoreVector> values, Context context) throws IOException, InterruptedException {

            submeida = "other";
            outputValue = null;
            for(MediaScoreVector val : values){
                if(val.getSubMedia()!=null && val.getSubMedia().length()>0){
                    submeida = val.getSubMedia();
                    context.getCounter("firstmr", "media").increment(1);
                } else {
                    outputValue = val.clone();
                    context.getCounter("firstmr", "weblog").increment(1);
                }
            }
            if(outputValue!=null){
                outputValue.setSubMedia(submeida);
                context.write(key, outputValue);
                if(submeida.equals("other")){
                    context.getCounter("firstmr", "other").increment(1);
                } else {
                    context.getCounter("firstmr", "normal").increment(1);
                }

            }

        }
    }


    public static class ChannelMapper extends Mapper<Text, MediaScoreVector, StringStringWritable, MediaScoreVector> {

        private StringStringWritable outputKey = new StringStringWritable();

        @Override
        public void map(Text key, MediaScoreVector value, Context context) throws IOException, InterruptedException {
            String channelName = MediaUrlRegexChannel.getChannelName(key.toString());
            outputKey.setFirst(value.getSubMedia());//子媒
            outputKey.setSecond(channelName);//渠道
            context.write(outputKey, value);
            context.getCounter("secondmr", "map").increment(1);
        }
    }



    public static class CountReducer extends Reducer<StringStringWritable, MediaScoreVector, StringStringWritable, MediaScoreVector> {

        private StringStringWritable outputKey = new StringStringWritable();
        private MediaScoreVector outputValue = null;
        int urlCount = 0;
        int pv = 0;
        int uv = 0;
        int gentiecount = 0;
        int gentieuv = 0;
        int backcount = 0;
        int sharecount = 0;

        @Override
        protected void reduce(StringStringWritable key, Iterable<MediaScoreVector> values, Context context) throws IOException, InterruptedException {
            urlCount = 0;
            pv = 0;
            uv = 0;
            gentiecount = 0;
            gentieuv = 0;
            backcount = 0;
            sharecount = 0;

            outputValue = new MediaScoreVector();
            for(MediaScoreVector val : values){
                if(val.getPv()>0){ //使用量 当天该媒体下有流量的文章数    pv不为0
                    urlCount++;
                    pv += val.getPv();
                    uv += val.getUv();
                    gentiecount += val.getGenTieCount();
                    gentieuv += val.getGenTieUv();
                    sharecount += val.getShareCount();
                    backcount += val.getBackCount();
                }

            }
            if(urlCount > 0){
                outputValue.setSubMedia(key.getFirst());
                outputValue.setChannel(key.getSecond());
                outputValue.setAllURLCount(urlCount);
                outputValue.setPv(pv);
                outputValue.setUv(uv);
                outputValue.setGenTieCount(gentiecount);
                outputValue.setGenTieUv(gentieuv);
                outputValue.setBackCount(backcount);
                outputValue.setShareCount(sharecount);
                context.write(key, outputValue);
                context.getCounter("secondmr", "reduce").increment(1);
            }

        }
    }




//    public static class CompanyMapper extends Mapper<LongWritable, Text, Text, MediaScoreVector> {
//
//        private Text outputKey = new Text();
//        private MediaScoreVector outputValue = new MediaScoreVector();
//
//
//        @Override
//        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            if(value.toString()!=null && value.toString().length()>0){
//                String[] array = value.toString().split("#@@#");
//                if(array.length>=2 && array[1]!=null && array[1].length()>0){
//                    outputKey.set(array[1]);
//                    if(array.length>=3){
//                        outputValue.setCompany(array[2]);
//                    } else {
//                        outputValue.setCompany("othercompany");
//                    }
//                    context.write(outputKey, outputValue);
//                    context.getCounter("thirdmr", "vaildmeid").increment(1);
//                } else {
//                    context.getCounter("thirdmr", "unvaildmeid"+value.toString()).increment(1);
//                }
//
//            }
//        }
//    }
//
//    public static class ResultMapper extends Mapper<StringStringWritable, MediaScoreVector, Text, MediaScoreVector> {
//
//        private Text outputKey = new Text();
//
//        @Override
//        public void map(StringStringWritable key, MediaScoreVector value, Context context) throws IOException, InterruptedException {
//            outputKey.set(key.getFirst());
//            value.setCompany("");
//            context.write(outputKey, value);
//            context.getCounter("thirdmr", "resultmapper").increment(1);
//        }
//    }


//    public static class ResultReducer extends Reducer<Text, MediaScoreVector, StringTriWritable, MediaScoreVector> {
//
//        private String company = null;
//        private StringTriWritable outputKey = new StringTriWritable();
//        private MediaScoreVector outputValue = null;
//        List<MediaScoreVector> tempList = null;
//
//        @Override
//        protected void reduce(Text key, Iterable<MediaScoreVector> values, Context context) throws IOException, InterruptedException {
//            company = "othercompany";
//            outputValue = null;
//            tempList = new ArrayList<MediaScoreVector>();
//            for(MediaScoreVector val : values){
//                if(val.getCompany()!=null && val.getCompany().length()>0){
//                    company = val.getCompany();
//                    context.getCounter("thirdmr", "company").increment(1);
//                } else {
//                    tempList.add(val.clone());
//                    context.getCounter("thirdmr", "chanelurl").increment(1);
//                }
//            }
//
//            //为子媒 channel url等 填充公司属性
//            for(MediaScoreVector val : tempList){
//                outputValue = val.clone();
//                context.getCounter("thirdmr", "submediaurl").increment(1);
//
//                outputKey.setFirst(company);
//                outputKey.setSecond(key.toString());
//                outputKey.setThird(outputValue.getChannel());
//                outputValue.setCompany(company);
//                context.write(outputKey, outputValue);//<company submedia channel , allurlcount pv uv....>
//                if(company.equals("othercompany")){
//                    context.getCounter("thirdmr", "other").increment(1);
//                } else {
//                    context.getCounter("thirdmr", "normal").increment(1);
//                }
//            }
//
//        }
//    }




}
