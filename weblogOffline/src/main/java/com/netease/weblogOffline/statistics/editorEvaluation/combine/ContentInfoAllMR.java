package com.netease.weblogOffline.statistics.editorEvaluation.combine;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.annotation.NotCheckInputFile;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.ContentAttributions;
import com.netease.weblogCommon.data.enums.Platform;
import com.netease.weblogCommon.data.enums.ShareBackChannel_EE;
import com.netease.weblogCommon.data.enums.SimpleDateFormatEnum;
import com.netease.weblogCommon.data.enums.StatisticsIndicator;
import com.netease.weblogCommon.tools.EditorEvaluationKeyBuilder;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.HashMapStringStringWritable;
import com.netease.weblogOffline.data.LongStringWritable;
import com.netease.weblogOffline.data.MultiStatisticResultWrapWritable;
import com.netease.weblogOffline.data.StatisticResultWritable;
import com.netease.weblogOffline.utils.HadoopUtils;
@NotCheckInputFile
public class ContentInfoAllMR extends MRJob {

	@Override
	public boolean init(String date) {

		try {
			String theDayBeforeYestrerday = DateUtils.getTheDayBefore(date, 1);

			inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION + "baseInfoAnd3gCombineTemp/" + date);
			inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION + "genTieAndPcAppWapInfoCombine/" + date);
			inputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION + "contentInfoAll/" + theDayBeforeYestrerday);

			outputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION + "contentInfoAll/" + date);

		} catch (ParseException e) {
			LOG.error(e);
		}

		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName()
				+ "_step1");
		
		job.getConfiguration().set("yesterday", getParams().getDate());
		boolean b =false;
		b|=addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, InfoCombineMapper.class);//中间数据
		b|=addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, AllInfoCombineMapper.class);//统计数据 + 中间数据
		b|=addInputPath(job, new Path(inputList.get(2)), SequenceFileInputFormat.class, ContentInfoAllMapper.class);//带活跃标记的中间数据
		
		if (!b){
			return jobState;
		}
		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongStringWritable.class);
       
		// reducer
		job.setReducerClass(ContentInfoAllReducer.class);
		job.setNumReduceTasks(16);
		FileOutputFormat.setOutputPath(job, new Path(outputList.get(0)));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(HashMapStringStringWritable.class);

		if (!job.waitForCompletion(true)) {
			jobState = JobConstant.FAILED;
		}
		return jobState;

	}
	
	public static class InfoCombineMapper extends Mapper<Text, HashMapStringStringWritable, Text, LongStringWritable> {
		
		private LongStringWritable outputValue = new LongStringWritable();

		@Override
		public void map(Text key, HashMapStringStringWritable value, Context context) throws IOException, InterruptedException {
			outputValue.setFirst(1);
			outputValue.setSecond(JsonUtils.toJson(value.getHm()));
			context.write(key, outputValue);
		}
		
	}

	public static class AllInfoCombineMapper extends Mapper<Text, MultiStatisticResultWrapWritable, Text, LongStringWritable> {
		private LongStringWritable outputValue = new LongStringWritable();
		private Text outKey = new Text();
		@Override
		public void map(Text key, MultiStatisticResultWrapWritable value, Context context) throws IOException, InterruptedException {
			outputValue.setFirst(2);
			
			StatisticResultWritable base_www = value.getMsr().getDataMap().get(new Text(EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.base, Platform.www, ShareBackChannel_EE.all)));
			StatisticResultWritable base_app = value.getMsr().getDataMap().get(new Text(EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.base, Platform.app, ShareBackChannel_EE.all)));
			StatisticResultWritable base_wap = value.getMsr().getDataMap().get(new Text(EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.base, Platform.wap, ShareBackChannel_EE.all)));
			
			if((null != base_www && base_www.getPv() > 0) || (null != base_app && base_app.getPv() > 0) || (null != base_wap && base_wap.getPv() > 0)){
				String s[] = key.toString().split(",");
				if (s.length==2){
					outKey.set(s[1]);
					outputValue.setSecond(JsonUtils.toJson(value.getMsr()));
					context.write(outKey, outputValue);
				}else if (s.length>2){
					String ks = "";
					for (int i = 1;i<s.length;i++){
						ks = ks +s[i];
					}
					outKey.set(ks);
					outputValue.setSecond(JsonUtils.toJson(value.getMsr()));
					context.write(outKey, outputValue);
				}

			}
		}
	}

	public static class ContentInfoAllMapper extends Mapper<Text, HashMapStringStringWritable, Text, LongStringWritable> {
		private LongStringWritable outputValue = new LongStringWritable();

		@Override
		public void map(Text key, HashMapStringStringWritable value, Context context) throws IOException, InterruptedException {
			outputValue.setFirst(3);
			outputValue.setSecond(value.getHm().get(ContentAttributions.activeFlag.getName()));
			context.write(key, outputValue);
		}
	}


	public static class ContentInfoAllReducer extends Reducer<Text, LongStringWritable, Text, HashMapStringStringWritable> {
		private HashMapStringStringWritable outputValue = new HashMapStringStringWritable();
		private Map<Long, String> map = new HashMap<Long, String>();
		private String dt;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			dt = context.getConfiguration().get("yesterday");
		}

		@Override
		protected void reduce(Text key, Iterable<LongStringWritable> values, Context context) throws IOException, InterruptedException {
			outputValue.getHm().clear();
			map.clear();
			for(LongStringWritable val : values){
				map.put(val.getFirst(), val.getSecond());
			}
			
			String attrs = map.get(1l);
			if(StringUtils.isNotBlank(attrs)){
				outputValue.getHm().putAll(JsonUtils.json2StrMap(attrs));//加入attr
			}else{
				context.getCounter("ContentInfoAllReducer", "haveNoConf").increment(1);
				return;
			}
			
			boolean isActive = (null != map.get(2l));//is active
			
			String activeFlagOld = map.get(3l);
			if(StringUtils.isBlank(activeFlagOld)){//新增
				activeFlagOld = ContentAttributions.ActiveFlagUtils.getDefaultActiveFlag();
			}
			
			String activeFlagNew = activeFlagOld;
			
			if(isActive){
				try {
					String publishDt = DateUtils.dateFormatTransform(outputValue.getHm().get(ContentAttributions.publishTime_3w.getName()), SimpleDateFormatEnum.zyLogTimeFormat.get(), SimpleDateFormatEnum.dateFormat.get());
					activeFlagNew = ContentAttributions.ActiveFlagUtils.updateActiveFlag(activeFlagOld,
							publishDt, dt);
				} catch (Exception e) {
					context.getCounter("ContentInfoAllReducer", "updateActiveFlagError").increment(1);
				}
			}
			
			outputValue.getHm().put(ContentAttributions.activeFlag.getName(), activeFlagNew);
			
			context.write(key, outputValue);
		}
	}
}










