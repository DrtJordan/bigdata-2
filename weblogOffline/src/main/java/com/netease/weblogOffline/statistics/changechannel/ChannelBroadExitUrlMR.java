package com.netease.weblogOffline.statistics.changechannel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.LoggerFactory;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.weblogfilter.WeblogFilterUtils;
import com.netease.weblogOffline.data.ThreeStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * Channel broad launch事件 exitUrl
 */

public class ChannelBroadExitUrlMR extends MRJob {

	private static final org.slf4j.Logger log = LoggerFactory
			.getLogger(ChannelBroadExitUrlMR.class);

	@Override
	public boolean init(String date) {
		// 输入列表
		// inputList.add(DirConstant.ZY_LOG + date);

		inputList.add(DirConstant.WEBLOG_FilterLOG
				+ date);

		// 输出列表
		outputList.add(DirConstant.WEBLOG_STATISTICS_TEMP_DIR
				+ "ChannelBroadExitUrl/" + date);

		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job = HadoopUtils
				.getJob(this.getClass(), this.getClass().getName());

		MultipleInputs.addInputPath(job, new Path(inputList.get(0)),
				SequenceFileInputFormat.class, LogMapper.class);

		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ThreeStringWritable.class);


		// reducer
		job.setReducerClass(ExitUrlReducer.class);
		job.setNumReduceTasks(16);
		FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(ThreeStringWritable.class);
		job.setOutputValueClass(IntWritable.class);

		if (!job.waitForCompletion(true)) {
			jobState = JobConstant.FAILED;
		}

		return jobState;
	}

	public static class LogMapper extends
			Mapper<NullWritable, Text, Text, ThreeStringWritable> {

		private Text outputkey = new Text();
		private Map<String, String> channelmap = new HashMap<String, String>();

		@Override
		public void map(NullWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			channelmap.clear();

			try {
				HashMap<String,String> lineMap = WeblogFilterUtils.buildKVMap(value.toString());

				String url = lineMap.get("url");
				String event = lineMap.get("event");
     
				NeteaseChannel_CS nce = NeteaseChannel_CS.getChannel(url);
				String channelName = "(null)";
                if (nce != null){
                	channelName = nce.getName();
                }
				if (event.equals("launch")) {
					String ref = lineMap.get("ref");
					String sid = lineMap.get("sid");
					String pgr = lineMap.get("pgr");
					String prev_pgr = lineMap.get("prev_pgr");
				
					String str[] = lineMap.get("project").split("@version@");
					String	broad ="(null)";
					if (str.length==2){
						broad = str[1];
					    }
						url = url.replace("_", "");
						ref= ref.replace("_", "");
						ThreeStringWritable puf = new ThreeStringWritable(pgr,channelName+"_"+broad+"_"+url,"0");
						ThreeStringWritable rpuf = new ThreeStringWritable(prev_pgr,channelName+"_"+broad+"_"+ref,"1");
						outputkey.set(sid);
						context.write(outputkey, puf);
						context.write(outputkey, rpuf);
				}

			} catch (Exception e) {
				context.getCounter("LogMapper", "mapException").increment(1);
			}
		}

	}

	public static class ExitUrlReducer extends
			Reducer<Text, ThreeStringWritable, ThreeStringWritable, IntWritable> {

		private IntWritable outputValue = new IntWritable(-1);

		@Override
		protected void reduce(Text key, Iterable<ThreeStringWritable> values,
				Context context) throws IOException, InterruptedException {
			HashMap<String ,ThreeStringWritable> hm = new HashMap<String ,ThreeStringWritable>();
			HashMap<String ,ThreeStringWritable> hm2 = new HashMap<String ,ThreeStringWritable>();

			for (ThreeStringWritable val : values) {
				String pgr = val.getfirst();
				ThreeStringWritable sss = new ThreeStringWritable(val.getfirst(),val.getsecond(),val.getthird());
				if (hm.get(pgr)==null){
					hm.put(pgr, sss);
					hm2.put(pgr, sss);
				}else{
					hm2.remove(pgr);
				}
			}
           for (String pgr :hm2.keySet()){
        
        	   if (hm2.get(pgr).getthird().equals("0")){
        		   String[] fst = hm2.get(pgr).getsecond().split("_");
        		   if(fst.length==3){
        				if ((!fst[0].equals("(null)"))&&(!fst[1].equals("(null)"))&&NeteaseContentType.artical.match(fst[2])){
            	  			context.write(new ThreeStringWritable(fst[0],fst[1],fst[2]), outputValue);
            	  		}   
        		   }
        	  	
        	   }
           }
	
		}
	}
}
