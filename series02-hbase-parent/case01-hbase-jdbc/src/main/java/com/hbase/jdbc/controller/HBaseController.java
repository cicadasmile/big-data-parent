package com.hbase.jdbc.controller;

import com.hbase.jdbc.config.HBaseConfig;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

@RestController
public class HBaseController {

    /**
     * 扫描全表
     */
    @GetMapping("/scanTable")
    public String scanTable () throws Exception {
        Table table = HBaseConfig.getTable(TableName.valueOf("user"));
        try {
            ResultScanner resultScanner = table.getScanner(new Scan());
            for (Result result : resultScanner) {
                printResult(result);
            }
        } finally {
            HBaseConfig.close(null, table);
        }
        return "success";
    }

    /**
     * 根据RowKey扫描
     */
    @GetMapping("/scanRowKey")
    public void scanRowKey() throws Exception {
        String rowKey = "id02";
        Table table = HBaseConfig.getTable(TableName.valueOf("user"));
        try {
            Result result = table.get(new Get(rowKey.getBytes()));
            printResult(result);
        } finally {
            HBaseConfig.close(null, table);
        }
    }

    /**
     * 输出 Result
     */
    private void printResult (Result result){
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        Set<Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> set = map.entrySet();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : set) {
            Set<Map.Entry<byte[], NavigableMap<Long, byte[]>>> entrySet = entry.getValue().entrySet();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entry2 : entrySet) {
                System.out.print(new String(result.getRow()));
                System.out.print("\t");
                System.out.print(new String(entry.getKey()));
                System.out.print(":");
                System.out.print(new String(entry2.getKey()));
                System.out.print(" value = ");
                System.out.println(new String(entry2.getValue().firstEntry().getValue()));
            }
        }
    }
}
