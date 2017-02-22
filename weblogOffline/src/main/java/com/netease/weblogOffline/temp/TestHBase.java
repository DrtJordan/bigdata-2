package com.netease.weblogOffline.temp;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;

import com.netease.weblogCommon.tools.EditorEvaluationKeyBuilder;
import com.netease.weblogCommon.utils.BytesUtils;
import com.netease.weblogOffline.statistics.editorEvaluation.hbase.HBaseUtil;

public class TestHBase {

    public static final String TABLE_NAME = "datacube:editor"; // 录入目的表名
    public static final String FAMILY = "values"; // 列簇
    public static final String QUALIFIER_CONF = "conf"; // conf列
    public static final String QUALIFIER_PVUV = "pvuv"; // uv列

    private static Configuration conf = HBaseUtil.getConfiguration();

    public static void main(String[] args) {
        HTable table = null;
        try {
            table = new HTable(conf, TABLE_NAME);
            Scan scan = new Scan();
            scan.setFilter(new PageFilter(50L));
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                String rowkey = new String(r.getRow());
                System.out.print(rowkey + " : ");
                List<Cell> confCells = r.getColumnCells(FAMILY.getBytes(), QUALIFIER_CONF.getBytes());
                for (Cell cell : confCells) {
                    Map<String, String> confMap = BytesUtils.bytesToSS(CellUtil.cloneValue(cell));
                    System.out.print("{");
                    for (Entry<String, String> entry : confMap.entrySet()) {
                        System.out.print(entry.getKey() + ":" + entry.getValue() + ",");
                    }
                    System.out.print("}");
                }
                System.out.print(" ");
                List<Cell> pvuvCells = r.getColumnCells(FAMILY.getBytes(), QUALIFIER_PVUV.getBytes());
                for (Cell cell : pvuvCells) {
                    Map<Integer, String> pvuvMap = BytesUtils.bytesToIS(CellUtil.cloneValue(cell));
                    System.out.print("{");
                    for (Entry<Integer, String> entry : pvuvMap.entrySet()) {
                        String columnName = EditorEvaluationKeyBuilder.uncompactColumnName(entry.getKey());
                        System.out.print(columnName + ":" + entry.getValue() + ",");
                    }
                    System.out.print("}");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
