package com.netease.weblogOffline.temp;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.common.PathFilters;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * doc count
 *
 */

public class DocidAndCountMR extends MRJob {

	@Override
	public boolean init(String date) {
        //输入列表
		inputList.add(DirConstant.GEN_TIE_INFO);

        //输出列表
        outputList.add("/ntes_weblog/weblog/statistics/temp/DocidAndCount" + date);
        outputList.add("/home/weblog/liyonghong/DocidAndCount/dociandcount.csv");

		return true;
	}
	
    @Override
    public int run(String[] args) throws Exception {
    	int jobState = JobConstant.SUCCESSFUL;
    	
    	Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName() + "-step1");
    	
     	//用户自定义广告曝光比例参数
    	Configuration conf = job1.getConfiguration();
    	
          DistributedCache.addCacheFile(new URI(getHDFS().getUri().toString() +"/ntes_weblog/weblog/statistics/temp/docid"+"#docid"), job1.getConfiguration());
          DistributedCache.createSymlink(job1.getConfiguration());
          getPaths(inputList.get(0),job1);

        
        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        
        //reducer
        job1.setReducerClass(DocidAndCountReducer.class);
        job1.setNumReduceTasks(16);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }
        loadSeqFileToLocal(job1,outputList.get(0),outputList.get(1));
        
        return jobState;
    }
    
    private static  void getPaths(String basedir,Job job2) throws IOException {
    
		BufferedReader br = null;
		  FileSystem fs= FileSystem.get(job2.getConfiguration());

		FSDataInputStream fin = fs.open(new Path("/ntes_weblog/weblog/statistics/temp/needDate.txt"));

	    br = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
	    String str = null;
		 while ((str = br.readLine())!=null){
			 try {
				 MultipleInputs.addInputPath(job2, new Path(basedir+str.trim()), TextInputFormat.class, DocidAndCountMapper.class);
			 }catch(Exception e ){
			
			 }

		 }
		try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

       // return commaSeparatedPaths;
      }
    
    
    private static  void loadSeqFileToLocal(Job job1,String src, String des) {
    	SequenceFile.Reader hdfsReader = null;
    	BufferedWriter writer = null;
    	Text key = new Text();
    	IntWritable value = new IntWritable();
		try {
			FileSystem	hdfs2 = FileSystem.get(job1.getConfiguration());
			writer = new BufferedWriter(new FileWriter(new File(des)));
			writer.write("排序\t,栏目\t,新闻事件\t,主题\t,外推标题\t,日期\t,类型\t,链接\t,docid\t,跟贴数量\t");
			writer.newLine();
			for(FileStatus status : hdfs2.listStatus(new Path(src),PathFilters.partFilter())){
				        hdfsReader = new SequenceFile.Reader(hdfs2, status.getPath(), job1.getConfiguration());
				    	while (hdfsReader.next(key, value)) {
				    		StringBuilder sb = new StringBuilder();
				            String[] str = key.toString().split("\\*-\\*");
				            String space = "";
				            for (int i=0;i<str.length;i++){
				            	sb.append(space).append(str[i]);
				            	space ="\t,";
				            }
				    		writer.write(sb.toString()+"\t,"+value.get());
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
    public static class DocidAndCountMapper extends Mapper<LongWritable, Text, Text,IntWritable> {
   
    	private static HashMap<String,String>  map;

        @Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
	      map = loadCacheFileToMap(context);
		}

		@Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       	try {

       		//        	url,pdocid,docid,发帖用户id,跟帖时间，跟帖id
       		        	String[] strs = value.toString().split(",");
       	
       		        	String docid = strs[2];
       		        	if (map.get(docid.trim())!=null){
       		        		context.write(new Text(map.get(docid.trim())), new IntWritable(1));
       		        	}

            } catch (Exception e) {
				context.getCounter("InfoOfPerPGRMapper", "parseError").increment(1);
			}
        }
		
		
		private HashMap<String,String> loadCacheFileToMap(Context context) throws IOException {
			HashMap<String,String> hm = new HashMap<String,String>();
			
			BufferedReader br = null;
		    br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("docid"))));
				 
				 String str = null;
				 str = br.readLine();//escape first line
				 while ((str = br.readLine())!=null){
					 try {
						 String[] docinfo = str.split("\\*-\\*");
						 if (!docinfo[8].trim().equals("")){
							 hm.put(docinfo[8].trim(), str); 
						 }
			
					 }catch(Exception e ){
						context.getCounter("InfoOfPerPGRMapper", "loadCacheFileToMap").increment(1);
					 }

				 }
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return hm;
		}
    }
    
    public static class DocidAndCountReducer extends Reducer<Text,IntWritable, Text, IntWritable> {
    	

    	
    
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
        	int sum = 0;
        	for(IntWritable val : values){
        		sum += val.get();
        	}
           context.write(new Text(key), new IntWritable(sum));
        }

    }
   
}





