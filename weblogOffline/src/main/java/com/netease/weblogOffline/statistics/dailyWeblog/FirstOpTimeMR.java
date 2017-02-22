package com.netease.weblogOffline.statistics.dailyWeblog;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
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
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.weblogfilter.WeblogFilterUtils;
import com.netease.weblogOffline.utils.HadoopUtils;

public class FirstOpTimeMR extends MRJob {
	@Override
	public boolean init(String date) {
		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
        
    	Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName());
        
        MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, LogMapper.class);
        
        //mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        //reducer
        job.setReducerClass(CountReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        if(!job.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        return jobState;
    }
    
    public static class LogMapper extends Mapper<NullWritable, Text, Text, Text> {
    	
    	private Text outputkey = new Text();
    	private Text outputValue = new Text();
    //	private LogParser logParser = new WeblogParser(); 
    	
        @Override
        public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        	try {
				HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());	

				String ptype = lineMap.get("ptype");
				String pver = lineMap.get("pver");
				String event = lineMap.get("event");
				if("ab_0".equals(ptype) && "b".equals(pver) 
						&& ("launch".equalsIgnoreCase(event) || "viewfocus".equalsIgnoreCase(event) || "click".equalsIgnoreCase(event) || "tabswitch".equalsIgnoreCase(event))){
		
					String uuid = lineMap.get("uuid");
					String pgr =  lineMap.get("pgr");
					String utime =  lineMap.get("utime");
					Map<String, String> map = new HashMap<String, String>();
					map.put("uuid", uuid);
					map.put("utime", utime);
					map.put("event", event);
					
					outputkey.set(pgr);
					outputValue.set(JsonUtils.toJson(map));
					context.write(outputkey, outputValue);
				}
			} catch (Exception e) {
				context.getCounter("LogMapper", "parseError").increment(1);
			}
        }
    }
    
    public static class CountReducer extends Reducer<Text, Text, Text, Text> {
    	
    	private Map<Long, Map<String, String>> map = new TreeMap<Long, Map<String,String>>();
    	private Text outputkey = new Text();
    	private Text outputValue = new Text();
    	
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	map.clear();
        	String uuid = null;
        	for(Text value : values){
        		try {
        			Map<String, String> mapT = JsonUtils.json2Map(value.toString());
        			map.put(Long.parseLong(mapT.get("utime")), mapT);
        			if(uuid == null){
        				uuid = mapT.get("uuid");
        			}
				} catch (Exception e) {
					e.printStackTrace();
				}
        	}
        	
        	long launchTime = 0;
        	long firstViewFocusTime = 0;
        	long firstClickTime = 0;
        	long firstTabSwitchTime = 0;
        	
        	for(Entry<Long, Map<String, String>> e : map.entrySet()){
        		if(launchTime == 0 && "launch".equalsIgnoreCase(e.getValue().get("event"))){
        			launchTime = e.getKey();
        		}else if(firstViewFocusTime == 0 && "viewfocus".equalsIgnoreCase(e.getValue().get("event"))){
        			firstViewFocusTime = e.getKey();
        		}else if(firstClickTime == 0 && "click".equalsIgnoreCase(e.getValue().get("event"))){
        			firstClickTime = e.getKey();
        		}else if(firstTabSwitchTime == 0 && "tabswitch".equalsIgnoreCase(e.getValue().get("event"))){
        			firstTabSwitchTime = e.getKey();
        		}
        	}
        	
        	
        	writeToFile("viewfocus", launchTime, firstViewFocusTime, context);
        	writeToFile("click", launchTime, firstClickTime, context);
        	writeToFile("tabswitch", launchTime, firstTabSwitchTime, context);
        	
        	
//        	if(launchTime > 0){
//        		String res = "viewfocus=" +  (firstViewFocusTime > 0 ? String.valueOf(firstViewFocusTime - launchTime) : "null")
//        				+ ",click=" + (firstClickTime > 0 ? String.valueOf(firstClickTime - launchTime) : "null") 
//        				+ ", tabswitch=" + (firstTabSwitchTime > 0 ? String.valueOf(firstTabSwitchTime - launchTime) : "null");
//        		outputkey.set(uuid + "_" + pgr);
//        		outputValue.set(res);
//        		context.write(outputkey, outputValue);
//        	}
        }
        
        private void writeToFile(String op, long launchTime, long opTime, Context context) throws IOException, InterruptedException{
        	if(launchTime > 0 && opTime > 0){
        		outputkey.set(op);
        		long diff = opTime - launchTime;
        		if(diff > 0){
        			outputValue.set(String.valueOf(diff));
        			context.write(outputkey, outputValue);
        		}
    		}
        }
    }
    
    private DecimalFormat df = new DecimalFormat("#0.0");
	private double step = 0.1d;
	private double maxTime = 60.0d;
    
    private String value2Index(String value){
    	return df.format(Long.parseLong(value) * 1.0 / 1000);
    }
    
    @Override
    public void extraJob(String[] args) {
    	try {
    		String dt = getParams().getDate();
			String hdfsFileDir =  outputList.get(0);
			String localFileName =  outputList.get(1);
			if(StringUtils.isBlank(hdfsFileDir) || StringUtils.isBlank(localFileName) ){
				return;
			}
			//<op,<120.3,count>>
			Map<String,Map<String,Integer>> map = new HashMap<String, Map<String,Integer>>();
			for(FileStatus s : getHDFS().listStatus(new Path(hdfsFileDir))){
				Path hdfsFilePath = s.getPath();
				if(hdfsFilePath.getName().startsWith("p")){
					@SuppressWarnings("deprecation")
					Reader reader = new Reader(getHDFS(), hdfsFilePath, new Configuration());
					Text key = new Text();
					Text val = new Text();
					while(reader.next(key, val)){
						String op = key.toString();
						String value = val.toString();
						Map<String, Integer> tempMap = map.get(op);
						if(null == tempMap){
							tempMap = new HashMap<String, Integer>();
							map.put(op, tempMap);
						}
						String index = value2Index(value); 
						Integer oldValue = tempMap.get(index);
						if(null == oldValue){
							oldValue = 0;
						}
						
						tempMap.put(index, oldValue + 1);
					}
					reader.close();
				}
			}
			for(String op : map.keySet()){
				Map<String,Integer> counterMap = map.get(op);
				BufferedWriter writer = new BufferedWriter(new FileWriter(new File(localFileName + "_" + op + "_" + dt)));
				for (double d = 0; d <= maxTime; d += step) {
					String ind = df.format(d);
					String from = ind + "s";
					String to = df.format(d + step) + "s";
					Integer count = counterMap.get(ind);
					if(null == count){
						count = 0;
					}
					
					writer.append(from + "-" + to +"\t"+ count);
					writer.newLine();
				}
				writer.flush();
				writer.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}












