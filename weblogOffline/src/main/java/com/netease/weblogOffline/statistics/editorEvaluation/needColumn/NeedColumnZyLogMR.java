package com.netease.weblogOffline.statistics.editorEvaluation.needColumn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogOffline.data.LongStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 提取章鱼日志中字段（url uid sid）
 * 
 */

public class NeedColumnZyLogMR extends MRJob {

    @Override
    public boolean init(String date) {
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;

        Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());

        MultipleInputs
                .addInputPath(job, new Path(inputList.get(0)), TextInputFormat.class, NeedColumnZyLogMapper.class);

        // mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongStringWritable.class);

        // reducer
        job.setReducerClass(NeedColumnZyLogReducer.class);
        job.setNumReduceTasks(64);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        if (!job.waitForCompletion(true)) {
            jobState = JobConstant.FAILED;
        }

        return jobState;
    }

    public static class NeedColumnZyLogMapper extends Mapper<LongWritable, Text, Text, LongStringWritable> {

        private Text outKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                HashMap<String, String> zylogParserMap = NeedColumnUtils.zylogParser(value.toString());
                LongStringWritable ls = new LongStringWritable();
                ls.setFirst(Long.parseLong(zylogParserMap.get("logTime")));
                ls.setSecond(zylogParserMap.get("url"));
                outKey.set(zylogParserMap.get("uid"));

                context.write(outKey, ls);

            } catch (Exception e) {
                context.getCounter("NeedColumnZyLogMapper", "parseError").increment(1);
            }

        }
    }

    public static class NeedColumnZyLogReducer extends Reducer<Text, LongStringWritable, Text, NullWritable> {

        private Text outputKey = new Text();

        private static long sessionThreshold = 30 * 60 * 1000;

        @Override
        protected void reduce(Text key, Iterable<LongStringWritable> values, Context context) throws IOException,
                InterruptedException {

            ArrayList<LongStringWritable> al = new ArrayList<LongStringWritable>();

            for (LongStringWritable val : values) {
                al.add(new LongStringWritable(val));
            }

            Collections.sort(al);

            long curSessionBeginTime = al.get(0).getFirst();
            long preActionTime = al.get(0).getFirst();
            outputKey.set(al.get(0).getSecond() + "\t" + key.toString() + "\t" + key.toString() + "_"
                    + curSessionBeginTime);

            context.write(outputKey, NullWritable.get());

            for (int i = 1; i < al.size(); ++i) {
                long curActionTime = al.get(i).getFirst();
                if (curActionTime - preActionTime > sessionThreshold) {
                    curSessionBeginTime = curActionTime;
                }
                preActionTime = curActionTime;
                outputKey.set(al.get(i).getSecond() + "\t" + key.toString() + "\t" + key.toString() + "_"
                        + curSessionBeginTime);
                context.write(outputKey, NullWritable.get());
            }

        }

    }
}
