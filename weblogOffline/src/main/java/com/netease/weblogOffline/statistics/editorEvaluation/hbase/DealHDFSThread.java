package com.netease.weblogOffline.statistics.editorEvaluation.hbase;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 多线程处理HDFS文件
 *
 */
public abstract class DealHDFSThread {
    
    /**
     * 执行入口
     * @param resDir HDFS目录
     * @param dt 日期
     * @throws IOException
     * @throws InterruptedException
     */
    public void execute(String resDir, final String dt) throws IOException, InterruptedException {
        if (!resDir.endsWith("/")) {
            resDir += "/";
        }

        Path dataDir = new Path(resDir + dt);

        FileSystem fs = FileSystem.get(new Configuration());

        if (!fs.exists(dataDir)) {
            System.out.println("not exists: " + dataDir);
            return;
        }
        
        FileStatus[] listStatus = fs.listStatus(dataDir);
        int fileNum = 0; //需要遍历的文件个数
        for (FileStatus fileStatus : listStatus) {
            Path path = fileStatus.getPath();
            if (path.getName().startsWith("p")) {
                fileNum++;
            }
        }
        
        final Map<String, Long> countMap = new ConcurrentHashMap<String, Long>();
        
        final CountDownLatch latch = new CountDownLatch(fileNum); //线程计数器
        long startTime = System.currentTimeMillis();
        
        for (FileStatus fileStatus : listStatus) {
            final Path path = fileStatus.getPath();
            if (path.getName().startsWith("p")) {
                Thread t = new Thread() {
                    @Override
                    public void run() {
                        try {
                            build(path, dt, countMap);
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            latch.countDown();
                        }
                    }
                };
                t.start();
            }
        }
        
        latch.await(); //等待所有线程执行完成
        long endTime = System.currentTimeMillis();
        
        long countTotal = 0L;
        for (Entry<String, Long> entry : countMap.entrySet()) {
            String fileName = entry.getKey();
            long count = entry.getValue();
            System.out.println("fileName=" + fileName + ", count=" + count);
            countTotal += count;
        }
        System.out.println("countTotal=" + countTotal + ", cost=" + (endTime - startTime) / 1000 + "s");
    }
    
    /**
     * 处理每个文件的方法
     * @param path 文件Path
     * @param dt 日期
     * @param countMap 计数器
     */
    protected abstract void build(Path path, String dt, Map<String, Long> countMap);
}
