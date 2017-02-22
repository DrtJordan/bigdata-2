package com.netease.weblogOffline.temp;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.slf4j.LoggerFactory;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.logparsers.ZyLogParams;
import com.netease.weblogCommon.logparsers.ZylogParser;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.utils.HadoopUtils;


/**
 * ChannelAndHomePvUv
 *
 */

public class OriginPvUvFromZyMR extends MRJob {
	
	private static final org.slf4j.Logger log = LoggerFactory.getLogger(OriginPvUvFromZyMR.class);

	@Override
	public boolean init(String date) {
        //输入列表
        inputList.add(DirConstant.ZY_LOG + date);
        
    	outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR + "OriginPvUvFromZyTemp/" + date);
        //输出列表
        outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR +"OriginPvUvFromZy/" + date);
 

		return true;
	}

    
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");


    	MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), TextInputFormat.class, LogMapper.class);
        
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        
        job1.setCombinerClass(PvUvTempReducer.class);
        
        //reducer
        job1.setReducerClass(PvUvTempReducer.class);
        job1.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
    	Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step2");

    	MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, PvUvMapper.class);
        
        //mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        
        //reducer
        job2.setReducerClass(PvUvReducer.class);
        job2.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    	
    	private LogParser logParser = new ZylogParser(); 
    	
    	private Text outputkey = new Text();
    	private IntWritable outputValue = new IntWritable(1);
    	private Map<String, String>  outputKeymap = new HashMap<String, String>();
        private Pattern baiduPattern = Pattern.compile("^http://[a-zA-Z0-9]+\\.hao123\\.com.*");

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	outputKeymap.clear();
        	
        	String oneLog = value.toString();
        	try {
        		Map<String, String> logMap = logParser.parse(oneLog);
				String url = logMap.get(ZyLogParams.url);
				String ref = logMap.get(ZyLogParams.reference);
				String uid =logMap.get(ZyLogParams.uid);
			   
				if ((url.indexOf("&qd=cs")!=-1)){
					outputKeymap.put("source", "太平洋军事文章页");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=mz")!=-1){
					outputKeymap.put("source", "116114导航");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=cc")!=-1){
					outputKeymap.put("source", "UC浏览器无线banner合作");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&from=sogou")!=-1){
					outputKeymap.put("source", "搜狗渠道");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=cl")!=-1){
					outputKeymap.put("source", "无线banner");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=wf")!=-1){
					outputKeymap.put("source", "电信无线");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=yd_top")!=-1){
					outputKeymap.put("source", "中国移动名站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=oppo_top")!=-1){
					outputKeymap.put("source", "OPPO浏览器名站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}				
				if (url.indexOf("?qd=mx")!=-1){
					outputKeymap.put("source", "魅族手机浏览器名站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=cs")!=-1){
					outputKeymap.put("source", "太平洋军事名站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=jinli")!=-1){
					outputKeymap.put("source", "金立翻翻");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=360ob")!=-1){
					outputKeymap.put("source", "360onebox");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?from=gionee")!=-1){
					outputKeymap.put("source", "金立手机浏览器名站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=ty")!=-1){
					outputKeymap.put("source", "123导航");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=vivo")!=-1){
					outputKeymap.put("source", "vivo手机浏览器宫格");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=h2345")!=-1){
					outputKeymap.put("source", "2345名站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=2345")!=-1){
					outputKeymap.put("source", "2345名站文章页");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=hovo")!=-1){
					outputKeymap.put("source", "联想浏览器名站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?from=sogou")!=-1){
					outputKeymap.put("source", "搜狗渠道酷站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?/&qd=gionee")!=-1){
					outputKeymap.put("source", "金立手机浏览器");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?lb=gionee")!=-1){
					outputKeymap.put("source", "金立手机浏览器宫格");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=hao123")!=-1){
					outputKeymap.put("source", "hao123WAP站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=woe")!=-1){
					outputKeymap.put("source", "联通wo门户娱乐");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=cp")!=-1){
					outputKeymap.put("source", "酷派手机浏览器名站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=lenovo_top")!=-1){
					outputKeymap.put("source", "联想浏览器频道首页");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=lb")!=-1){
					outputKeymap.put("source", "猎豹浏览器");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=mg")!=-1){
					outputKeymap.put("source", "魅族手机浏览器宫格");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=jinli")!=-1){
					outputKeymap.put("source", "金立开机push渠道");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=wom")!=-1){
					outputKeymap.put("source", "联通wo门户名站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=zsdx_page")!=-1){
					outputKeymap.put("source", "掌上大学文章页");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=h123bd")!=-1){
					outputKeymap.put("source", "hao123本地");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=wok")!=-1){
					outputKeymap.put("source", "联通wo门户酷站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=sm")!=-1){
					outputKeymap.put("source", "神马卡片");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=gzgx_page")!=-1){
					outputKeymap.put("source", "贵州高校文章页");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=hxin")!=-1){
					outputKeymap.put("source", "夏新手机浏览器名站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=zsdx_top")!=-1){
					outputKeymap.put("source", "掌上大学名站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=lt_view")!=-1){
					outputKeymap.put("source", "联通浙江视频");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=360ov")!=-1){
					outputKeymap.put("source", "360无线视频直播");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=uc_top")!=-1){
					outputKeymap.put("source", "UC浏览器名站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=sh_top")!=-1){
					outputKeymap.put("source", "上海热线名站");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&qd=xin")!=-1){
					outputKeymap.put("source", "夏新手机浏览器");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?/&qd=sh_page")!=-1){
					outputKeymap.put("source", "上海热线");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=us")!=-1){
					outputKeymap.put("source", "UC浏览器体育首页");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=cg")!=-1){
					outputKeymap.put("source", "酷派手机浏览器宫格");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}								
				if (url.indexOf("&qd=gdmap")!=-1){
					outputKeymap.put("source", "高德地图");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("&sg_push")!=-1){
					outputKeymap.put("source", "搜狗浏览器push");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				if (url.indexOf("?qd=yd_news")!=-1){
					outputKeymap.put("source", "中国移动新闻");
					outputKeymap.put("uid", uid);
					outputkey.set(JsonUtils.toJson(outputKeymap));
					context.write(outputkey, outputValue);
				}
				
	
			} catch (Exception e) {
				context.getCounter("LogMapper", "mapException").increment(1);
			}
        }
       
    }
    
    public static class PvUvTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
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
    
    public static class PvUvMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
    	private Text outputKey = new Text();
    	
        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        	Map<String,String> map = JsonUtils.json2Map(key.toString());
        	outputKey.set(map.get("source"));
        	context.write(outputKey, value);
        }
    }
    
    public static class PvUvReducer extends Reducer<Text, IntWritable, Text, Text> {
    	

    	private HashMap<String,HashMap<String,Integer>> sourcemap = new HashMap<String, HashMap<String,Integer>>();
    	private final String intDef = "0" ;
    	
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	
        	int pv = 0;
        	int uv = 0;
        	
        	for(IntWritable val : values){
        		pv += val.get();
        		uv++;
        	}
        	
        	
            String keyString = key.toString();
			HashMap<String,Integer> pumap = new HashMap<String, Integer>();
    		pumap.put("pv", pv);
        	pumap.put("uv", uv);
        	sourcemap.put(keyString, pumap);

        }


		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		    for(String source:sourcemap.keySet()){
		    	HashMap<String,Integer> tempmap = sourcemap.get(source);
	    		StringBuilder sb = new StringBuilder();
		    	if (tempmap.get("pv")!=null){
			    	sb.append(tempmap.get("pv")).append("\t");
		    	}else {
		    		sb.append(intDef).append("\t");
		    	}
		    	if (tempmap.get("uv")!=null){
			    	sb.append(tempmap.get("uv")).append("\t");
		    	}else {
		    		sb.append(intDef).append("\t");
		    	}	
		    	context.write(new Text(source), new Text(sb.toString()));
		    }
		}
  
    }
    	  
	  
}


