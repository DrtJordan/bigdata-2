package com.netease.weblogOffline.statistics.editorEvaluation.combine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import com.netease.weblogCommon.data.enums.ContentAttributions;
import com.netease.weblogCommon.data.enums.NeteaseContentType_EE;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.ArticleWritable;
import com.netease.weblogOffline.data.ColumnOFContent_3gWritable;
import com.netease.weblogOffline.data.Content_3gWritable;
import com.netease.weblogOffline.data.HashMapStringStringWritable;
import com.netease.weblogOffline.data.PhotoSetWritable;
import com.netease.weblogOffline.data.SpecialWritable;
import com.netease.weblogOffline.data.VidioWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

public class BaseInfoAnd3gCombineMRTemp extends MRJob {

	@Override
	public boolean init(String date) {
		inputList.add(DirConstant.LOG_OF_3G_ALL + date);
		inputList.add(DirConstant.ARTICLE_ALL + date);
		inputList.add(DirConstant.SPECIAL_ALL + date);
		inputList.add(DirConstant.PHOTOSET_ALL + date);
		inputList.add(DirConstant.VIDIO_ALL + date);
		outputList.add(DirConstant.WEBLOG_STATISTICS_EDITOR_EVALUATION + "baseInfoAnd3gCombineTemp/" + date);
		return true;
	}

	@Override
	public int run(String[] args) throws Exception {
		int jobState = JobConstant.SUCCESSFUL;

		Job job = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "_step1");

		MultipleInputs.addInputPath(job, new Path(inputList.get(0)), SequenceFileInputFormat.class, BaseInfoAnd3gCombine_3gMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputList.get(1)), SequenceFileInputFormat.class, BaseInfoAnd3gCombine_ArticleMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputList.get(2)), SequenceFileInputFormat.class, BaseInfoAnd3gCombine_SpecialMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputList.get(3)), SequenceFileInputFormat.class, BaseInfoAnd3gCombine_photoSetMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputList.get(4)), SequenceFileInputFormat.class, BaseInfoAnd3gCombine_VidioMapper.class);
		
		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(HashMapStringStringWritable.class);

		// reducer
		job.setReducerClass(BaseInfoAnd3gCombineReducer.class);
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

	public static class BaseInfoAnd3gCombine_3gMapper extends Mapper<Text, Content_3gWritable, Text, HashMapStringStringWritable> {
		HashMapStringStringWritable outValue = new HashMapStringStringWritable();
		List<String> types = new ArrayList<String>();
		List<String> channels = new ArrayList<String>();
		List<String> titles = new ArrayList<String>();
		Text outKey = new Text();

		@Override
		public void map(Text key, Content_3gWritable value, Context context) throws IOException, InterruptedException {
			outValue.getHm().clear();
			types.clear();
			channels.clear();
			titles.clear();
			
			for (ColumnOFContent_3gWritable cc : value.getContent_3g_list()) {
				types.add(cc.getType());
				channels.add(cc.getClolumnName());
				titles.add(cc.getTitle());
				
			}
			
			outValue.getHm().put(ContentAttributions.type_3g.getName(), types.toString());
			outValue.getHm().put(ContentAttributions.docid_3g.getName(), value.getDocid());
			outValue.getHm().put(ContentAttributions.url_3g.getName(), value.getGurl());
			outValue.getHm().put(ContentAttributions.channel_3g.getName(), channels.toString());
			outValue.getHm().put(ContentAttributions.title_3g.getName(), titles.toString());
			String editor = value.getEditor().trim();
			for(int i=0;i<types.size();++i){
				String s = types.get(i).trim();
			    
				if ("photoset".equals(s)){
					String ids[] =  value.getContent_3g_list().get(i).getId().split("\\|");
					if (ids.length == 2) {
						String id = ids[0].substring(ids[0].length()-4, ids[0].length())+"_"+ids[1];
						outValue.getHm().put(ContentAttributions.id_3g.getName(), id);//图集id: 频道id_图集id
						outValue.getHm().put(ContentAttributions.editor_3g.getName(),editor);
						outKey.set(outValue.getHm().get(ContentAttributions.id_3g.getName()));
						context.write(outKey, outValue);//<id,val>
					}
				}else if ("special".equals(s) || "video".equals(s)){
					outValue.getHm().put(ContentAttributions.id_3g.getName(), value.getContent_3g_list().get(i).getId());//专题id、视频id
					outValue.getHm().put(ContentAttributions.editor_3g.getName(),editor);
					outKey.set(outValue.getHm().get(ContentAttributions.id_3g.getName()));
					context.write(outKey, outValue);//<id,val>
				}else if ("normal".equals(s)){
					outValue.getHm().put(ContentAttributions.id_3g.getName(), outValue.getHm().get(ContentAttributions.docid_3g.getName()));
					outValue.getHm().put(ContentAttributions.editor_3g.getName(),editor);
					outKey.set(value.getwUrl());
					context.write(outKey, outValue);//<url,val>
				}else if ("webview".equals(s)){
					outValue.getHm().put(ContentAttributions.id_3g.getName(), outValue.getHm().get(ContentAttributions.docid_3g.getName()));//h5id
					outValue.getHm().put(ContentAttributions.editor_3g.getName(),editor);
					outKey.set(value.getContent_3g_list().get(i).getId());
					context.write(outKey, outValue);//<h5id,val>
				}
			}
		}
	}

	public static class BaseInfoAnd3gCombine_ArticleMapper extends Mapper<Text, ArticleWritable, Text, HashMapStringStringWritable> {
		private HashMapStringStringWritable outValue = new HashMapStringStringWritable();
		@Override
		public void map(Text key, ArticleWritable value, Context context) throws IOException, InterruptedException {
			outValue.getHm().clear();
			outValue.getHm().put(ContentAttributions.type_3w.getName(), NeteaseContentType_EE.article.getName());
			outValue.getHm().put(ContentAttributions.url_3w.getName(), value.getArticleUrl());
			outValue.getHm().put(ContentAttributions.id_3w.getName(), value.getDocId());
			outValue.getHm().put(ContentAttributions.author.getName(), value.getOriginalAuthor());
			outValue.getHm().put(ContentAttributions.isOriginal.getName(), value.getSourceTag());
			outValue.getHm().put(ContentAttributions.channel_3w.getName(), value.getChannel());
			outValue.getHm().put(ContentAttributions.editor_3w.getName(), value.getEditor());
			outValue.getHm().put(ContentAttributions.title_3w.getName(), value.getTitle());
			outValue.getHm().put(ContentAttributions.publishTime_3w.getName(), value.getPublishTime());
			context.write(key, outValue);//<url,val>
		}
	}

	public static class BaseInfoAnd3gCombine_SpecialMapper extends Mapper<Text, SpecialWritable, Text, HashMapStringStringWritable> {
		private HashMapStringStringWritable outValue = new HashMapStringStringWritable();
		@Override
		public void map(Text key, SpecialWritable value, Context context) throws IOException, InterruptedException {
			outValue.getHm().clear();
			outValue.getHm().put(ContentAttributions.type_3w.getName(), NeteaseContentType_EE.special.getName());
			outValue.getHm().put(ContentAttributions.url_3w.getName(), value.getUrl());
			outValue.getHm().put(ContentAttributions.author.getName(), value.getOriginalAuthor());
			outValue.getHm().put(ContentAttributions.isOriginal.getName(), value.getSourceTag());
			outValue.getHm().put(ContentAttributions.channel_3w.getName(), value.getChannel());
			outValue.getHm().put(ContentAttributions.editor_3w.getName(), value.getEditor());
			outValue.getHm().put(ContentAttributions.title_3w.getName(), value.getChineseName());
			outValue.getHm().put(ContentAttributions.publishTime_3w.getName(), value.getPublishTime());
			context.write(key, outValue);//<url,val>
		}
	}

	public static class BaseInfoAnd3gCombine_photoSetMapper extends Mapper<Text, PhotoSetWritable, Text, HashMapStringStringWritable> {
		private HashMapStringStringWritable outValue = new HashMapStringStringWritable();
		@Override
		public void map(Text key, PhotoSetWritable value, Context context) throws IOException, InterruptedException {
			outValue.getHm().clear();
			outValue.getHm().put(ContentAttributions.type_3w.getName(), NeteaseContentType_EE.photoSet.getName());
			outValue.getHm().put(ContentAttributions.id_3w.getName(), value.getChannel() + "_" + value.getSetId());//图集id: 频道id_图集id
			outValue.getHm().put(ContentAttributions.url_3w.getName(), value.getSetUrl());
			outValue.getHm().put(ContentAttributions.author.getName(), value.getOriginalAuthor());
			outValue.getHm().put(ContentAttributions.source.getName(), value.getSource());
			outValue.getHm().put(ContentAttributions.isOriginal.getName(), value.getOriginalType());
			outValue.getHm().put(ContentAttributions.channel_3w.getName(), value.getChannel());
			outValue.getHm().put(ContentAttributions.editor_3w.getName(), value.getEditor());
			outValue.getHm().put(ContentAttributions.title_3w.getName(), value.getSetName());
			outValue.getHm().put(ContentAttributions.publishTime_3w.getName(), value.getCreateTime());
			context.write(key, outValue);//<id,val>
		}
	}

	public static class BaseInfoAnd3gCombine_VidioMapper extends Mapper<Text, VidioWritable, Text, HashMapStringStringWritable> {
		private HashMapStringStringWritable outKey = new HashMapStringStringWritable();
		@Override
		public void map(Text key, VidioWritable value, Context context) throws IOException, InterruptedException {
			outKey.getHm().clear();
			outKey.getHm().put(ContentAttributions.type_3w.getName(), NeteaseContentType_EE.video.getName());
			outKey.getHm().put(ContentAttributions.id_3w.getName(), value.getvId());
			outKey.getHm().put(ContentAttributions.url_3w.getName(), value.getUrl());
			outKey.getHm().put(ContentAttributions.author.getName(), value.getOriginalAuthor());
			outKey.getHm().put(ContentAttributions.editor_3w.getName(), value.getEditor());
			outKey.getHm().put(ContentAttributions.title_3w.getName(), value.getTitle());
			outKey.getHm().put(ContentAttributions.publishTime_3w.getName(), value.getUpTime());
			context.write(key, outKey);//<id,val>
		}
	}

	public static class BaseInfoAnd3gCombineReducer extends Reducer<Text, HashMapStringStringWritable, Text, HashMapStringStringWritable> {
		private HashMapStringStringWritable outValue = new HashMapStringStringWritable();
		private Set<String> type_3wList = new HashSet<String>();
		private Text outKey = new Text();

		@Override
		protected void reduce(Text key, Iterable<HashMapStringStringWritable> values, Context context) throws IOException, InterruptedException {
			outValue.getHm().clear();
			type_3wList.clear();
			
			for (HashMapStringStringWritable val : values) {
				String type_3w = val.getHm().get(ContentAttributions.type_3w.getName());
				if (type_3w != null) {
					type_3wList.add(type_3w);
				}
				outValue.getHm().putAll(val.getHm());
			}

			if (type_3wList.size() < 2) {
				String type = outValue.getHm().get(ContentAttributions.type_3w.getName());
				String url_3w = outValue.getHm().get(ContentAttributions.url_3w.getName());
				outKey.set(key);
				if(!NeteaseContentType_EE.article.getName().equals(type) && url_3w != null){
					outKey.set(url_3w);
				}
				
				if (!outKey.toString().equals("")){
					context.write(outKey, outValue);
				}
			}
		}
	}
}
