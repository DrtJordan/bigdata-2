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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.logparsers.ZyLogParams;
import com.netease.weblogCommon.logparsers.ZylogParser;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.utils.HadoopUtils;


public class ChannelClassifyPvUvMR extends MRJob {

    @Override
    public boolean init(String date) {
        inputList.add(DirConstant.ZY_LOG + date);
        outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "ChannelClassifyPvUv4CMS/" + date);
        outputList.add(DirConstant.WEBLOG_STATISTICS_TODC_DIR + "cmspvuv/" + date);
        return true;
    }

    @Override
    public int run(String[] arg0) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;

        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "step-1");

        DistributedCache.addCacheFile(new URI(getHDFS().getUri().toString()
                + "/ntes_weblog/weblog/cache/cmsdata/domainchannel.txt" + "#domainchannel"), job1.getConfiguration());
        DistributedCache.addCacheFile(new URI(getHDFS().getUri().toString()
                + "/ntes_weblog/weblog/cache/cmsdata/homeurl.txt" + "#homeurl"), job1.getConfiguration());

        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), TextInputFormat.class, LogMapper.class);

        // mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setCombinerClass(PvUvTempReducer.class);

        // reducer
        job1.setReducerClass(PvUvTempReducer.class);
        job1.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        if (!job1.waitForCompletion(true)) {
            jobState = JobConstant.FAILED;
        }

        Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step2");

        MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, PvUvMapper.class);

        // mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        // reducer
        job2.setReducerClass(PvUvReducer.class);
        job2.setNumReduceTasks(8);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        if (!job2.waitForCompletion(true)) {
            jobState = JobConstant.FAILED;
        }

        return jobState;
    }

    public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private LogParser logParser = new ZylogParser();

        Map<String, String> domainChannelMap = new HashMap<String, String>();
        Set<String> domainUrls = new HashSet<String>();
        Set<String> homeUrls = new HashSet<String>();

        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable(1);
        private Map<String, String> map = new HashMap<String, String>();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException,
                InterruptedException {
            loadDomainChannelFile(context);
            loadHomeUrlFile(context);
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

        private void loadHomeUrlFile(Context context) throws IOException {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("homeurl"))));
            String line = null;
            while ((line = br.readLine()) != null) {
                homeUrls.add(line);
            }
            br.close();
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                Map<String, String> logMap = logParser.parse(line);
                String url = logMap.get(ZyLogParams.url);
                if (StringUtils.isBlank(url)) {
                    return;
                }
                for (String domainUrl : domainUrls) {
                    if (url.startsWith(domainUrl)) { // 判断URL以哪个域开头，就算到对应频道
                        String channelId = domainChannelMap.get(domainUrl);
                        String uid = logMap.get(ZyLogParams.uid);

                        map.clear();
                        map.put("channel", channelId);
                        map.put("type", "all");
                        map.put("uid", uid);
                        outputKey.set(JsonUtils.toJson(map));
                        context.write(outputKey, outputValue);

                        map.clear();
                        map.put("channel", channelId);
                        map.put("type", getUrlType(url));
                        map.put("uid", uid);
                        outputKey.set(JsonUtils.toJson(map));
                        context.write(outputKey, outputValue);
                    }
                }
            } catch (Exception e) {
                context.getCounter("LogMapper", "mapException:" + e.getMessage()).increment(1);
            }

        }
        
        private String getUrlType(String url) {
            String type = null;
            String pureUrl = UrlUtils.removeAnchor(url);
            if (pureUrl.endsWith("/")) {
                pureUrl = pureUrl.substring(0, pureUrl.length() - 1);
            }

            if (homeUrls.contains(pureUrl)) {
                type = "home";
            } else {
                type = NeteaseContentType.getTypeName(url);
            }

            return type;
        }

    }

    public static class PvUvTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }

    }

    public static class PvUvMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        private Text outputKey = new Text();
        private Map<String, String> keyMap = new HashMap<String, String>();

        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            keyMap.clear();
            Map<String, String> map = JsonUtils.json2Map(key.toString());
            keyMap.put("channel", map.get("channel"));
            keyMap.put("type", map.get("type"));
            outputKey.set(JsonUtils.toJson(keyMap));
            context.write(outputKey, value);
        }
    }

    public static class PvUvReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int pv = 0;
            int uv = 0;
            for (IntWritable val : values) {
                pv += val.get();
                uv++;
            }

            Map<String, String> keyMap = JsonUtils.json2Map(key.toString());

            outputKey.set(keyMap.get("channel") + "_pv_" + keyMap.get("type"));
            outputValue.set(pv);
            context.write(outputKey, outputValue);

            outputKey.set(keyMap.get("channel") + "_uv_" + keyMap.get("type"));
            outputValue.set(uv);
            context.write(outputKey, outputValue);

        }
    }

}
