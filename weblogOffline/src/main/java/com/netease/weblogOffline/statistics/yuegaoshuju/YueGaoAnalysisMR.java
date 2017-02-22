package com.netease.weblogOffline.statistics.yuegaoshuju;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.LoggerFactory;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.PathFilters;
import com.netease.weblogOffline.data.ContentScoreVector;
import com.netease.weblogOffline.data.StringTuple;
import com.netease.weblogOffline.data.YueGaoInfo;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 约稿数据
 *
 */

public class YueGaoAnalysisMR extends MRJob {
	
	private static final org.slf4j.Logger log = LoggerFactory.getLogger(YueGaoAnalysisMR.class);

	@Override
	public boolean init(String date) {
        //输入列表
        inputList.add("/ntes_weblog/weblog/temp/YueGaoAnalysis/article/" + date);
        
        inputList.add("/ntes_weblog/weblog/midLayer/contentScoreVector/");

        //输出列表
        outputList.add("/ntes_weblog/weblog/temp/YueGaoAnalysis/filterArticle/"+date);
        outputList.add("/ntes_weblog/weblog/temp/YueGaoAnalysis/result/"+date);
        outputList.add("/home/weblog/liyonghong/yuegao/yuegaocsv"+date+".csv");

		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
    	
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName()+"step1");
    	
    	
    	Configuration conf =job1.getConfiguration();
    	
    	//用户自定义参数
        int days = Integer.parseInt(this.getUserDefineParam("days", "10"));
        conf.setInt("days",days);
        
        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), TextInputFormat.class, URLOfEachDayMapper.class);
        
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
       
        
        //reducer
        job1.setReducerClass(URLOfEachDayReducer.class);
        job1.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(StringTuple.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        
        
	    Job job2 = HadoopUtils.getJob(this.getClass(), this.getClass().getName()+"step2");
    	
    	Configuration conf2 =job2.getConfiguration(); 
    	
    	
        conf2.set("filterAticle",outputList.get(0)+"/");
        
     
        // FileInputFormat.addInputPath(job2, );
      //  FileInputFormat.setInputPaths(job1, getCommaSeparatedPaths(outputList.get(0)+"/",inputList.get(1)));
         MultipleInputs.addInputPath(job2, new Path(inputList.get(0)), TextInputFormat.class, InfoOfEachDayMapper.class);
 
         getPaths(outputList.get(0)+"/",inputList.get(1),job2);
        
        //mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(YueGaoInfo.class);
        
        
        //combiner
        job2.setCombinerClass(AddOfPerUrlCombiner.class);
        
        //reducer
        job2.setReducerClass(AddOfPerUrlReducer.class);
        job2.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job2, new Path(outputList.get(1)));
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        if(!job2.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        loadSeqFileToLocal(job2,outputList.get(1),outputList.get(2));
    
        return jobState;
    }
    
    private static  void loadSeqFileToLocal(Job job2,String src, String des) {
    	SequenceFile.Reader hdfsReader = null;
    	BufferedWriter writer = null;
    	Text key = new Text();
    	Text value = new Text();
		try {
			FileSystem	hdfs2 = FileSystem.get(job2.getConfiguration());
			writer = new BufferedWriter(new FileWriter(new File(des)));
			writer.write("月份\t,频道\t,作者\t,标题\t,url\t,联系编辑\t,发布日期\t,10内累计PV\t,10内累计UV\t,10内累计跟帖次数\t,10内累计跟帖人数\t,10内累计分享次数\t,10内累计回流次数\t");
			writer.newLine();
			for(FileStatus status : hdfs2.listStatus(new Path(src),PathFilters.partFilter())){
				        hdfsReader = new SequenceFile.Reader(hdfs2, status.getPath(), job2.getConfiguration());
				    	while (hdfsReader.next(key, value)) {
				    		String valueString = value.toString().replaceAll("\t", " ");
				    		valueString = valueString.replace(",", " ");
				    	    valueString = valueString.replace("#@@#", "\t,");
				    		writer.write(valueString);
				    		writer.newLine();
				}
			}	
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			if (hdfsReader!=null ){
				hdfsReader.close();
			}
			if (writer!=null){
				writer.close();
			}
	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
    private static  void getPaths(String filterArticle, String basedir,Job job2) {
    	SequenceFile.Reader hdfsReader = null;
    
    	Text key = new Text();
    	StringTuple value = new StringTuple();
  	   // StringBuilder commaSeparatedPaths=new  StringBuilder();
  	   // String sep = "";
  	   
		try {
			FileSystem	hdfs = FileSystem.get(new Configuration());
			for(FileStatus status : hdfs.listStatus(new Path(filterArticle))){
				if(status.getPath().getName().startsWith("p")){
				        hdfsReader = new SequenceFile.Reader(hdfs, status.getPath(), new Configuration());
				    	while (hdfsReader.next(key, value)) {
				        if (hdfs.exists(new Path(basedir+key.toString()+"/"))){
				        	MultipleInputs.addInputPath(job2, new Path(basedir+key.toString()), SequenceFileInputFormat.class, AddOfPerUrlMapper.class);
				         }
				   }		
				}
			}
		
	  
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (hdfsReader!=null ){
			try {
				hdfsReader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
       // return commaSeparatedPaths;
      }
    
    
    
    public static class URLOfEachDayMapper extends Mapper<LongWritable, Text, Text,Text> {
   
    	private static final String splitRegex = "#@@#";
    	private static final String regex = "#@@#";
    	private static int days;
    	
    	private static final List<String> vaildPrefixs = new ArrayList<String>();
    	static {
    		vaildPrefixs.add("http://help.3g.163.com");
    		vaildPrefixs.add("http://3g.163.com");
    	}
    	
        @Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
	      days = context.getConfiguration().getInt("days", 10);
		}

		@Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       	try {
       		    String[] strs = value.toString().split(splitRegex);
			    String impureUrl = strs[strs.length - 2];
			    String url = UrlUtils.getOriginalUrl(impureUrl.trim()).trim();
				StringBuilder sb = new StringBuilder();
				String sep = "";
				for(int i = 0; i < strs.length; ++i){
					sb.append(sep).append(strs[i]);
					sep = regex;
				}
				if(NeteaseContentType.artical.match(url) && isValidUrl(url)){
					String	ct = UrlUtils.getAticalCt(url);
					
					String toDt = DateUtils.getTheDayBefore(ct, -(days - 1));
					List<String> latestDateList = DateUtils.getDateList(ct, toDt);
				    for (String dt : latestDateList){
				    	context.write(new Text(dt), new Text(url));
				    }
				}
          
            } catch (Exception e) {
				context.getCounter("URLOfEachDayMapper", "parseError").increment(1);
			}      	
        }
		
		private static boolean isValidUrl(String url){
			for(String prefix : vaildPrefixs){
				if(url.startsWith(prefix)){
					return false;
				}
			}		
			return true;
		}
    }
    
    public static class URLOfEachDayReducer extends Reducer<Text,Text, Text, StringTuple> {
    	
    	private Text outKey = new Text();
    	
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
        	
            StringTuple st = new StringTuple();

        	for(Text val : values){	
                st.add(new String(val.toString()));
        	}  
        	outKey.set(key);
        	context.write(outKey, st);
        }
    }
	
    public static class InfoOfEachDayMapper extends Mapper<LongWritable, Text, Text,YueGaoInfo> {
    	   
    	private static final String splitRegex = "#@@#";
    	private static final String regex = "#@@#";
    

    	private static final List<String> vaildPrefixs = new ArrayList<String>();
    	static {
    		vaildPrefixs.add("http://help.3g.163.com");
    		vaildPrefixs.add("http://3g.163.com");
    	}
    	


		@Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    try {
       		    String[] strs = value.toString().split(splitRegex);
			    String impureUrl = strs[strs.length - 2];
			    String url = UrlUtils.getOriginalUrl(impureUrl.trim()).trim();
				StringBuilder sb = new StringBuilder();
				String sep = "";
				for(int i = 0; i < strs.length; ++i){
					sb.append(sep).append(strs[i]);
					sep = regex;
				}
	    
				if(NeteaseContentType.artical.match(url) && isValidUrl(url)){
					String	ct = UrlUtils.getAticalCt(url);	
					YueGaoInfo outValue=  new YueGaoInfo();
				    outValue.setUrlinfo(sb.append(regex).append(ct).toString());
					context.write(new Text(url), outValue);
				}
          
            } catch (Exception e) {
            	e.printStackTrace();
				context.getCounter("InfoOfEachDayMapper", "parseError").increment(1);
			}      	
        }
		
		private static boolean isValidUrl(String url){
			for(String prefix : vaildPrefixs){
				if(url.startsWith(prefix)){
					return false;
				}
			}		
			return true;
		}
    }
	
	  public static class AddOfPerUrlMapper extends Mapper<Text, ContentScoreVector, Text,YueGaoInfo> {
		   
	    	private static HashMap<String,List<String>>  dateAndUrlsmap;
	   
	    
	        @Override
			protected void setup(Context context) throws IOException,
					InterruptedException {
	        	Configuration conf = context.getConfiguration();
	        	String filterArticle = conf.get("filterAticle");
	        	dateAndUrlsmap = loadSeqFileToMap(context,filterArticle);
			}

			@Override
	        public void map(Text key, ContentScoreVector value, Context context) throws IOException, InterruptedException {
				try {
			    InputSplit split = context.getInputSplit();
			    Class<? extends InputSplit> splitClass = split.getClass();
			    
			     FileSplit fileSplit = null;
		        if (splitClass.equals(FileSplit.class)) {
			     fileSplit = (FileSplit) split;
		        } else if (splitClass.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
	
			   
				  Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
				  getInputSplitMethod.setAccessible(true);
				  fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
		        }
	        	 Path path =fileSplit.getPath();
	       	     String date = path.getParent().getName();
	       	 	 YueGaoInfo outValue=  new YueGaoInfo();
	       	 	 if (key.toString().indexOf("http://play")!=-1){
	       	 		String s =key.toString().replace("http://play", "http://gg");
	       	 	    if (dateAndUrlsmap.get(date)!=null&&dateAndUrlsmap.get(date).contains(key.toString())){
       				outValue.setPv(value.getPv());
       				outValue.setUv(value.getUv());
       				outValue.setGenTieCount(value.getGenTieCount());
       				outValue.setGenTieUv(value.getGenTieUv());
       				outValue.setShareCount(value.getShareCount());
       				outValue.setBackCount(value.getBackCount());
	       			context.write(key, outValue);
       			    }else if (dateAndUrlsmap.get(date)!=null&&dateAndUrlsmap.get(date).contains(s)){
	       				outValue.setPv(value.getPv());
	       				outValue.setUv(value.getUv());
	       				outValue.setGenTieCount(value.getGenTieCount());
	       				outValue.setGenTieUv(value.getGenTieUv());
	       				outValue.setShareCount(value.getShareCount());
	       				outValue.setBackCount(value.getBackCount());
		       			context.write(new Text(s), outValue);
	       			    } 
	       	 		
	       	 	 }else {
	        	    	if (dateAndUrlsmap.get(date)!=null&&dateAndUrlsmap.get(date).contains(key.toString())){
	 		       				outValue.setPv(value.getPv());
	 		       				outValue.setUv(value.getUv());
	 		       				outValue.setGenTieCount(value.getGenTieCount());
	 		       				outValue.setGenTieUv(value.getGenTieUv());
	 		       				outValue.setShareCount(value.getShareCount());
	 		       				outValue.setBackCount(value.getBackCount());
	 			       			context.write(key, outValue);
	 		       			}
	       	 	 }
	

	            } catch (Exception e) {
	            	
					context.getCounter("AddOfPerUrlMapper", "parseError").increment(1);
					
				}
	        }
			
			
			private HashMap<String,List<String>> loadSeqFileToMap(Context context,String filterArticle) {
				HashMap<String,List<String>> hm = new HashMap<String,List<String>>();
				
				SequenceFile.Reader hdfsReader = null;
		    	Text key = new Text();
		    	StringTuple value = new StringTuple();
	
				try {
					FileSystem	hdfs2 = FileSystem.get(context.getConfiguration());
//					for(FileStatus status : hdfs2.listStatus(new Path(filterArticle),PathFilters.partFilter())){
//						        hdfsReader = new SequenceFile.Reader(hdfs2, status.getPath(), context.getConfiguration());
//						    	while (hdfsReader.next(key, value)) {
//						    		hm.put(key.toString(), value.getEntries());
//						}
//					}	
					
					for(FileStatus status : hdfs2.listStatus(new Path(filterArticle))){
						if(status.getPath().getName().startsWith("p")){
						        hdfsReader = new SequenceFile.Reader(hdfs2, status.getPath(), context.getConfiguration());
						    	while (hdfsReader.next(key, value)) {
						    		hm.put(key.toString(), value.getEntries());
						   }		
						}
					}	
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				try {
					if (hdfsReader!=null ){
						hdfsReader.close();
					}
			
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return hm;
			}
	    }
	  
	  
	  
		public static class AddOfPerUrlCombiner extends Reducer<Text, YueGaoInfo, Text, YueGaoInfo> {
			
	 
	
			@Override
			protected void reduce(Text key, Iterable<YueGaoInfo> values, Context context) throws IOException, InterruptedException {
			    YueGaoInfo outValue=  new YueGaoInfo();
				for (YueGaoInfo v : values) {
					if (v.getUrlinfo().equals("")){
						outValue.setPv(new Integer(outValue.getPv()+v.getPv()));
						outValue.setUv(new Integer(outValue.getUv()+v.getUv()));
						outValue.setGenTieCount(new Integer(outValue.getGenTieCount()+v.getGenTieCount()));
						outValue.setGenTieUv(new Integer(outValue.getGenTieUv()+v.getGenTieUv()));
						outValue.setShareCount(new Integer(outValue.getShareCount()+v.getShareCount()));
						outValue.setBackCount(new Integer(outValue.getBackCount()+v.getBackCount()));
				
					}else {
					 context.write(key, v);	
					}
				  
				}
				  
			    context.write(key, outValue);	

			}
		}
	    
	  
	  public static class AddOfPerUrlReducer extends Reducer<Text, YueGaoInfo, Text, Text> {
	     	
		
	     	private static final String regex = "#@@#";

	    
	    	@Override
			protected void reduce(Text key, Iterable<YueGaoInfo> values, Context context) throws IOException, InterruptedException {
	    		
	    		YueGaoInfo outValue=  new YueGaoInfo();
				String urlInfo=new String();
				
		
				for (YueGaoInfo v : values) {
					
					if (v.getUrlinfo().equals("")){
						
						outValue.setPv(new Integer(outValue.getPv()+v.getPv()));
						outValue.setUv(new Integer(outValue.getUv()+v.getUv()));
						outValue.setGenTieCount(new Integer(outValue.getGenTieCount()+v.getGenTieCount()));
						outValue.setGenTieUv(new Integer(outValue.getGenTieUv()+v.getGenTieUv()));
						outValue.setShareCount(new Integer(outValue.getShareCount()+v.getShareCount()));
						outValue.setBackCount(new Integer(outValue.getBackCount()+v.getBackCount()));
					
					}else {
						urlInfo= new String ( v.getUrlinfo());
					}
				    
				}
			   
				StringBuilder sb = new StringBuilder();
				sb.append(urlInfo).append(regex).append(outValue.getPv()).append(regex).append(outValue.getUv()).append(regex).append(outValue.getGenTieCount()).append(regex).append(outValue.getGenTieUv()).append(regex).append(outValue.getShareCount()).append(regex).append(outValue.getBackCount());
			    context.write(key, new Text(sb.toString()));
			}

	    }
}





