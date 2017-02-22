package com.netease.weblogOffline.exp;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.netease.jurassic.hadoopjob.MRJob;
import com.netease.jurassic.hadoopjob.data.JobConstant;
import com.netease.weblogOffline.common.DirConstant;
import com.netease.weblogOffline.data.MediaScoreVector;
import com.netease.weblogOffline.data.StringStringWritable;
import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 将数据格式化后文件
 * Created by hfchen on 2015/4/9.
 */
public class MediaExpFileMR extends MRJob{
    @Override
    public boolean init(String date) {
        //输入列表
        inputList.add(DirConstant.MEDIA_STATISTICS_DIR + "mediamerge/" + date);
        outputList.add(DirConstant.MEDIA_STATISTICS_DIR + "mediaexpfile/" + date);
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        int jobState = JobConstant.SUCCESSFUL;

        //合并子媒与url的信息
        Job job1 = HadoopUtils.getJob(this.getClass(), this.getClass().getName());

        MultipleInputs.addInputPath(job1, new Path(inputList.get(0)), SequenceFileInputFormat.class,
            MediaMapper.class);

        //mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(NullWritable.class);

        job1.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job1, new Path(outputList.get(0)));

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);

        if(!job1.waitForCompletion(true)){
            jobState = JobConstant.FAILED;
        }

        return jobState;
    }


    public static class MediaMapper extends Mapper<StringStringWritable, MediaScoreVector, Text, NullWritable> {

        private Text outputKey = new Text();
        String separateS = "#@@#";


        @Override
        public void map(StringStringWritable key, MediaScoreVector media, Context context) throws IOException, InterruptedException {
            outputKey.set(media.getSubMedia() + separateS
                + media.getChannel() + separateS
                + media.getAllURLCount() + separateS
                + media.getPv() + separateS
                + media.getUv() + separateS
                + media.getGenTieCount() + separateS
                + media.getGenTieUv() + separateS
                + media.getBackCount() + separateS
                + media.getShareCount());
            context.write(outputKey, NullWritable.get());
        }
    }




    /**
//     * 执行完mr后的额外工作，如数据导出本地，会返任务执行状态
//     * */
//
//    @Override
//    public int extraJobWithStatus(String[] args){//extraJobOnly
//        LOG.info(" extrajob begin");
//        FileSystem fs = getHDFS();
//        String date = getParams().getDate();
////        String path = DirConstant.MEDIA_STATISTICS_DIR + "mediamerge/" + date + "/";
//        String path = "/ntes_weblog/media/statistics/mediamerge/" + date + "/";
//        Path pathP = new Path(path); //杭州
//        LOG.info("prepare read hdfs file " + pathP.getName());
//        String separateS = "#@@#";
//
//        String outputPath = "/home/weblog/webAnalysis/mediaFile/pc_"+ date;
//        if(args.length==1){
//            outputPath = args[0];
//        }
//
//        File outputFile = new File(outputPath);
//        //存在文件先删除
//        if(outputFile.exists()){
//            outputFile.delete();
//        }
//        try {
//            FileWriter fw = new FileWriter(outputPath);
//            if (fs!=null && fs.exists(pathP)) {
//                FileStatus[] listStatus = fs.listStatus(pathP);
//                SequenceFile.Reader reader = null;
//                String s = null;
//                for (Path p: FileUtil.stat2Paths(listStatus)) {
//                    String fn = p.getName();
//                    if (fn.startsWith("part")) {
//                        //读取序列化文件
//                        LOG.info("read the hdfs file now.the name is " + fn);
//                        reader = new SequenceFile.Reader(getHDFS(), p, getConf());
//                        StringStringWritable key = new StringStringWritable();
//                        Writable value = (MediaScoreVector) reader.getValueClass().newInstance();
//                        while (reader.next(key, value)) {
//                            MediaScoreVector media = (MediaScoreVector)value;
//                            s = media.getSubMedia() + separateS
//                              + media.getChannel() + separateS
//                              + media.getAllURLCount() + separateS
//                              + media.getPv() + separateS
//                              + media.getUv() + separateS
//                              + media.getGenTieCount() + separateS
//                              + media.getGenTieUv() + separateS
//                              + media.getBackCount() + separateS
//                              + media.getShareCount() ;
//                            fw.write(s.toString());
//                            fw.write("\r\n");//写入换行
//
//                        }
//
//
//                    }
//                }
//            }
//            try {
//                fw.close();
//                fw = null;
//            } catch (IOException e) {
//                LOG.error("close writer error", e);
//                return 12;
//            }
//        } catch (Exception e) {
//            LOG.error("read from hdfs error", e);
//            return 12;
//        }
//        LOG.info("import data to file finished.");
//
//        return 0;
//    }
}
