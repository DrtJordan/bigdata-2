package com.netease.weblogOffline.statistics.contentscore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.logparsers.WeblogParser;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.weblogfilter.WeblogFilterUtils;
import com.netease.weblogOffline.data.StringLongSemiHash;
import com.netease.weblogOffline.data.YcInfo;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 统计各频道首页访问pvuvnv，点击pvuvcountnv数据
 * 计算大数据日志频道首页，频道整体专题的pvuv等指标
 *
 *
 * HQL参考语句：
 1）PV：weblogHive "select count(uuid) from weblog where dt=日期 and event=\"launch\" and (url like “该频道首页正则”);"
 2）UV：weblogHive "select count(distinct uuid) from weblog where dt=日期 and event=\"launch\" and (url like “该频道首页正则”)
 3）NV：weblogHive "select count(distinct sid) from weblog where dt=日期 and event=\"launch\" and (url like “该频道首页正则”)
 4）click数量：weblogHive "select count(uuid) from weblog where dt=日期 and event=\"click\" and (url like “该频道首页正则”)
 5）有click的PV数量：weblogHive "select count(pgr) from weblog where dt=日期 and event=\"click\" and (url like “该频道首页正则”)
 6）有click的UV数量：weblogHive "select count(distinct uuid) from weblog where dt=日期 and event=\"click\" and (url like“该频道首页正则”)
 7）有click的NV数量：weblogHive "select count(distinct sid) from weblog where dt=日期 and event=\"click\" and (url like“该频道首页正则”)
 *
 *新流程20150116日上线
 */

public class ChannelHomeFromWeblogMR extends MRJob {
	
    private final static String SPECIALSPLIT = "_#$%";

	@Override
	public boolean init(String date) {
        //输入列表
     //   inputList.add(DirConstant.WEBLOG_LOG + date);//weblog
	    String WeblogOrFilter = this.getUserDefineParam("WeblogOrFilter", "filter");
		if (WeblogOrFilter.equals("weblog")){
			inputList.add(DirConstant.WEBLOG_LOG + date);//weblog
		}else {
			inputList.add(DirConstant.WEBLOG_FilterLOG
					+ date);
		}
	
        inputList.add(DirConstant.YC_INFO_ALL + date);//ycInfo
        //输出列表
        outputList.add(DirConstant.WEBLOG_STATISTICS_OTHER_DIR + "ChannelHomeFromWeblogTemp/" + date);
        outputList.add(DirConstant.WEBLOG_STATISTICS_OTHER_DIR + "ChannelHomeFromWeblog/" + date);
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        String WeblogOrFilter = this.getUserDefineParam("WeblogOrFilter", "filter");
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");
    	
        if (WeblogOrFilter.equals("weblog")){
               MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), TextInputFormat.class, WeblogMapper.class);
        }else {
        	   MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class, LogMapper.class);
		}
    
        
          
        MultipleInputs.addInputPath(job1, new Path(inputList.get(1)), SequenceFileInputFormat.class, YcInfoMapper.class);
        
        //mapper
        job1.setMapOutputKeyClass(StringLongSemiHash.class);
        job1.setMapOutputValueClass(Text.class);
        
        //reducer
        job1.setReducerClass(AddYcInfoReducer.class);
        job1.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
    	Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step2");
        
        MultipleInputs.addInputPath(job2, new Path(outputList.get(0)), SequenceFileInputFormat.class, CountMapper.class);
        
        //mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        
        job2.setCombinerClass(Combiner.class);
        
        //reducer
        job2.setReducerClass(CountReducer.class);
        job2.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class YcInfoMapper extends Mapper<Text, YcInfo, StringLongSemiHash, Text> {
    	
    	private StringLongSemiHash outputKey = new StringLongSemiHash();
    	private Text outputValue = new Text();
    	
    	protected void setup(Context context) throws IOException, InterruptedException {
    		Map<String, String> map = new HashMap<String, String>();
    		map.put("source", "yc");
    		
    		outputValue.set(JsonUtils.toJson(map));
    	}
    	
        @Override
        public void map(Text key, YcInfo value, Context context) throws IOException, InterruptedException {
        	outputKey.setFirst(key.toString());
        	outputKey.setSecond(Long.MIN_VALUE);
        	
        	context.write(outputKey, outputValue);
        }
    }
    
    public static class LogMapper extends Mapper<NullWritable, Text, StringLongSemiHash, Text> {
    	
    	private StringLongSemiHash outputKey = new StringLongSemiHash();
    	private Text outputValue = new Text();
    	
    	
        @Override
        public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {

          		HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());
           	    
       	     String event = lineMap.get("event");

                if("launch".equals(event) ||"click".equals(event)){
                	Map<String, String> logMapSimple = new HashMap<String, String>();
        			String sid = lineMap.get("sid");
					String url =  lineMap.get("url");
			        String uuid =  lineMap.get("uuid");
				    String pgr =  lineMap.get("pgr");
                	logMapSimple.put("url", url);
                	logMapSimple.put("event", event);
                	logMapSimple.put("uuid", uuid);
                	logMapSimple.put("sid", sid);
                	logMapSimple.put("pgr", pgr);
                	
                	outputKey.setFirst(UrlUtils.getOriginalUrl(url));
                	outputKey.setSecond(0);
                	
                	outputValue.set(JsonUtils.toJson(logMapSimple));
                	
                	context.write(outputKey, outputValue);
                	
                }
            } catch (Exception e) {
				context.getCounter("LogMapper", "parseError").increment(1);
			}
        }
    }
    
    
    public static class WeblogMapper extends Mapper<LongWritable, Text, StringLongSemiHash, Text> {
    	
    	private StringLongSemiHash outputKey = new StringLongSemiHash();
    	private Text outputValue = new Text();
    	
    	private LogParser logParser = new WeblogParser();
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
				Map<String, String> logMap = logParser.parse(value.toString());
     
           	    
   
				String event = logMap.get("event");
                if("launch".equals(event) ||"click".equals(event)){
                	Map<String, String> logMapSimple = new HashMap<String, String>();
        			String sid =logMap.get("sid");
					String url = logMap.get("url");
			        String uuid = logMap.get("uuid");
				    String pgr = logMap.get("pgr");
                	logMapSimple.put("url", url);
                	logMapSimple.put("event", event);
                	logMapSimple.put("uuid", uuid);
                	logMapSimple.put("sid", sid);
                	logMapSimple.put("pgr", pgr);
                	
                	outputKey.setFirst(UrlUtils.getOriginalUrl(url));
                	outputKey.setSecond(0);
                	
                	outputValue.set(JsonUtils.toJson(logMapSimple));
                	
                	context.write(outputKey, outputValue);
                	
                }
            } catch (Exception e) {
				context.getCounter("WeblogMapper", "parseError").increment(1);
			}
        }
    }
    
    public static class AddYcInfoReducer extends Reducer<StringLongSemiHash, Text, Text, Text> {
    	
    	private Text outputKey = new Text();
    	private Text outputValue = new Text();
    	
    	private String url = null;
    	
        @Override
        protected void reduce(StringLongSemiHash key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	String curUrl = key.getFirst();
        	long flag = key.getSecond();
        	outputKey.set(curUrl);
        	
        	if(flag == Long.MIN_VALUE){
        		url = curUrl;
        		return;
        	}

        	boolean isYc = false;
        	
        	if(curUrl.equals(url)){//yc
        		isYc = true;
        		context.getCounter("AddYcInfoReducer", "ycUrlInLog").increment(1);;
        	}
        	
        	url = null;
        	
        	for(Text val : values){
        		if(isYc){
        			Map<String, String> tempMap= JsonUtils.json2Map(val.toString());
        			tempMap.put("source", "yc");
        			outputValue.set(JsonUtils.toJson(tempMap));
        		}else{
        			outputValue.set(val);
        		}
        		
        		context.write(outputKey, outputValue);
        	}
        	
        }
    }
    
    public static class CountMapper extends Mapper<Text, Text, Text, IntWritable> {
    	
    	private Text outputkey = new Text();
    	private IntWritable outputValue = new IntWritable(1);
    	
        private String channeltype = "";
    	
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	try {
				Map<String, String> logMap = JsonUtils.json2Map(value.toString());
				String event = logMap.get("event");
	            if("launch".equals(event) ||"click".equals(event)){
                    String url = logMap.get("url");
                    NeteaseChannel_CS nc = NeteaseChannel_CS.getChannel(url);
//                    NeteaseExamChannel nceHome = NeteaseExamChannel.getChannelNameByChannelHome(url);//频道首页
//                    NeteaseExamChannel nceAll = NeteaseExamChannel.getChannelName(url);//频道整体
                    boolean isSpecial = NeteaseContentType.getTypeName(url).equals("special")? true : false;//是否专题首页
                    String type = "launch".equals(event) ? "a": "c";

                    if(nc != null){
                        channeltype = "";
                        if(nc.isHome(url)){//频道首页
                            channeltype = "home";
                            setChannelInfo(context, logMap, nc.getName(), type, channeltype);
                        }
                        
                        //频道整体
                        channeltype = "all";
                        setChannelInfo(context, logMap, nc.getName(), type, channeltype);
                        if(isSpecial && !"yc".equals(logMap.get("source"))){//是否专题首页
                            channeltype = "special";
                            setChannelInfo(context, logMap, nc.getName(), type, channeltype);
                        }

                        if(channeltype!=null && channeltype.length()>0){
                            context.getCounter("LogMapper", "ncematch").increment(1);
                        } else {
                            context.getCounter("LogMapper", "ncenotnull").increment(1);
                        }

                    } else {
                        context.getCounter("LogMapper", "ncenotnull").increment(1);
                    }
                }
//                if("launch".equals(event) ||"click".equals(event)){
//                    String url = logMap.get("url");
//                    NeteaseExamChannel nceHome = NeteaseExamChannel.getChannelNameByChannelHome(url);//频道首页
//                    NeteaseExamChannel nceAll = NeteaseExamChannel.getChannelName(url);//频道整体
//                    boolean isSpecial = NeteaseContentType.getTypeName(url).equals("special")? true : false;//是否专题首页
//                    String type = "launch".equals(event) ? "a": "c";
//
//                    if(nceHome != null || nceAll != null){
//                        channeltype = "";
//                        if(nceHome != null && nceHome.matchHome(url)){//频道首页
//                            channeltype = "home";
//                            setChannelInfo(context, logMap, nceHome.getName(), type, channeltype);
//                        }
//                        if(nceAll!=null){//频道整体
//                            channeltype = "all";
//                            setChannelInfo(context, logMap, nceAll.getName(), type, channeltype);
//                            if(isSpecial && !"yc".equals(logMap.get("source"))){//是否专题首页
//                                channeltype = "special";
//                                setChannelInfo(context, logMap, nceAll.getName(), type, channeltype);
//                            }
//                        }
//
//                        if(channeltype!=null && channeltype.length()>0){
//                            context.getCounter("LogMapper", "ncematch").increment(1);
//                        } else {
//                            context.getCounter("LogMapper", "ncenotnull").increment(1);
//                        }
//
//                    } else {
//                        context.getCounter("LogMapper", "ncenotnull").increment(1);
//                    }
//                }
            } catch (Exception e) {
				context.getCounter("LogMapper", "parseError").increment(1);
                context.getCounter("LogMapper", "Error" + e.getMessage()).increment(1);
			}
        }

        /**
         * 设置channle信息
         * @param context
         * @param logMap
         * @param channelName
         * @param type
         * @param channeltype
         * @throws IOException
         * @throws InterruptedException
         */
        private void setChannelInfo(Context context, Map<String, String> logMap, String channelName, String type, String channeltype) throws IOException, InterruptedException {
            if(channeltype!=null && channeltype.length()>0){
                String uuid = logMap.get("uuid");
                String sid = logMap.get("sid");

                outputkey.set(channeltype + "" + channelName + "_" + type + "_uuid" + SPECIALSPLIT + uuid);
                context.write(outputkey, outputValue);

                outputkey.set(channeltype + "" + channelName + "_" + type + "_sid" + SPECIALSPLIT + sid);
                context.write(outputkey, outputValue);

                if(type.length()>0){
                    String pgr = logMap.get("pgr");
                    outputkey.set(channeltype + "" + channelName + "_" + type + "_pgr" + SPECIALSPLIT + pgr);
                    context.write(outputkey, outputValue);
                }
            }
        }
    }
    
	public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable outputValue = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable v : values) {
				sum += v.get();
			}
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}
    
    public static class CountReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    	
    	private Map<String, Integer> uniqueMap = new TreeMap<String, Integer>();//去重
    	private Map<String, Integer> sumMap = new TreeMap<String, Integer>(); //求和
        
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	String k = key.toString();
        	String prefix = k.substring(0, k.indexOf(SPECIALSPLIT) + 1);

            //处理求和事件
        	if(prefix.contains("uuid")){
                int sum = 0;
                for (IntWritable v : values) {
                    sum += v.get();
                }
                String mapKey = "";
                if(prefix.contains("_c_")){ //click事件，uuid求和属于count操作
                    mapKey = prefix.replace("uuid","count");
                } else if(prefix.contains("_a_")){ //launch事件，uuid求和属于pv操作
                    mapKey = prefix.replace("uuid","pv");
                }
                if(mapKey.length()>0){
                    int alreadycount = sumMap.get(mapKey)==null ? 0 : sumMap.get(mapKey).intValue();
                    sumMap.put(mapKey, alreadycount + sum);
                }
            }

			//求distinct
            String mapKey = "";
            if(prefix.contains("_c_")) { //click事件
                mapKey = prefix.replace("pgr","pv");
                mapKey = mapKey.replace("sid","nv");
                mapKey = mapKey.replace("uuid","uv");

            } else if(prefix.contains("_a_")){ //launch事件
                mapKey = prefix.replace("sid","nv");
                mapKey = mapKey.replace("uuid","uv");

            }
            if(mapKey.length()>0) {
                int alreadycount = uniqueMap.get(mapKey) == null ? 0 : uniqueMap.get(mapKey).intValue();
                uniqueMap.put(mapKey, alreadycount + 1);
            }
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<String, Map<String,Integer>> allMap = new HashMap<String, Map<String, Integer>>();
            for(Entry<String, Integer> e : uniqueMap.entrySet()) {
                String channel = e.getKey().substring(0, e.getKey().indexOf("_"));
                if(allMap.containsKey(channel)){
                    allMap.get(channel).put(e.getKey(), e.getValue());
                } else {
                    Map<String,Integer> map = new HashMap<String, Integer>();
                    map.put(e.getKey(),e.getValue());
                    allMap.put(channel, map);
                }
            }
            for(Entry<String, Integer> e : sumMap.entrySet()) {
                String channel = e.getKey().substring(0, e.getKey().indexOf("_"));
                if(allMap.containsKey(channel)){
                    allMap.get(channel).put(e.getKey(), e.getValue());
                } else {
                    Map<String,Integer> map = new HashMap<String, Integer>();
                    map.put(e.getKey(),e.getValue());
                    allMap.put(channel, map);
                }
            }
            String channel = null;
            int launchPV = 0;
            int launchUV = 0;
            int launchNV = 0;
            int clickCOUNT = 0;
            int clickPV = 0;
            int clickUV = 0;
            int clickNV = 0;
            double avgclick = 0d; // 人均点击次数  click数量/UV
            double avgvisit = 0d; // 人均访问次数  NV/UV
            double avgclickvisit = 0d; // 人均有点击访问次数 click数量/有click NV

            Text outpultkey = new Text();
            StringBuffer sb = new StringBuffer();
            for(Entry<String, Map<String,Integer>> e : allMap.entrySet()){
                launchPV = 0;
                launchUV = 0;
                launchNV = 0;
                clickCOUNT = 0;
                clickPV = 0;
                clickUV = 0;
                clickNV = 0;
                channel = e.getKey();
                for(Entry<String,Integer> entry: e.getValue().entrySet()){
                    if(entry.getKey().contains("_a_")){ //launch
                        if(entry.getKey().contains("pv")){
                            launchPV = entry.getValue();
                        } else if(entry.getKey().contains("uv")){
                            launchUV = entry.getValue();
                        } else if(entry.getKey().contains("nv")){
                            launchNV = entry.getValue();
                        }
                    } else  if(entry.getKey().contains("_c_")){ //click
                        if(entry.getKey().contains("pv")){
                            clickPV = entry.getValue();
                        } else if(entry.getKey().contains("uv")){
                            clickUV = entry.getValue();
                        } else if(entry.getKey().contains("nv")){
                            clickNV = entry.getValue();
                        } else if(entry.getKey().contains("count")){
                            clickCOUNT = entry.getValue();
                        }
                    }
//                    outpultkey.set(entry.getKey());//
//                    context.write(outpultkey, new IntWritable(entry.getValue()));//

                }
                avgclick = Math.round((((double)clickCOUNT)/launchUV)*100)/100d;// 人均点击次数  click数量/UV Math.round((((double)clickCOUNT)/launchUV)*100)/100d
                avgvisit = Math.round((((double)launchNV)/launchUV)*100)/100d; // 人均访问次数  NV/UV
                avgclickvisit = Math.round((((double)clickCOUNT)/clickNV)*100)/100d; // 人均有点击访问次数 click数量/有click NV
                sb = new StringBuffer();
                sb.append(channel).append("\t").append(launchPV).append("\t").append(launchUV).append("\t").append(launchNV).append("\t")
                        .append(clickCOUNT).append("\t").append(clickPV).append("\t").append(clickUV).append("\t").append(clickNV)
                        .append("\t").append(String.format("%.2f", avgclick)).append("\t").append(String.format("%.2f", avgvisit)).append("\t").append(String.format("%.2f", avgclickvisit));
                outpultkey.set(sb.toString());
//                context.write(outpultkey, outputvalue);
                context.write(outpultkey, NullWritable.get());

            }

    	}
    }
}





