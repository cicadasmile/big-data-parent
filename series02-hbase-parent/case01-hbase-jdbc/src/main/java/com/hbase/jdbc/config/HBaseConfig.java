package com.hbase.jdbc.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class HBaseConfig {

    private static String address;
    private static final Object lock=new Object();
    public static Configuration configuration = null;
    public static ExecutorService executor = null;
    public static Connection connection = null;

    /**
     * 获取连接
     */
    public static Connection getConnection(){
        if(null == connection){
            synchronized (lock) {
                if(null == connection){
                    configuration = new Configuration();
                    configuration.set("hbase.zookeeper.quorum", address);
                    try {
                        executor = Executors.newFixedThreadPool(10);
                        connection = ConnectionFactory.createConnection(configuration, executor);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return connection;
    }

    /**
     * 获取 HBaseAdmin
     */
    public static HBaseAdmin getHBaseAdmin(){
        HBaseAdmin admin = null;
        try{
            admin = (HBaseAdmin)getConnection().getAdmin();
        }catch(Exception e){
            e.printStackTrace();
        }
        return admin;
    }
    /**
     * 获取 Table
     */
    public static Table getTable(TableName tableName) {
        Table table = null ;
        try{
            table = getConnection().getTable(tableName);
        }catch(Exception e){
            e.printStackTrace();
        }
        return table ;
    }
    /**
     * 关闭资源
     */
    public static void close(HBaseAdmin admin,Table table){
        try {
            if(admin!=null) {
                admin.close();
            }
            if(table!=null) {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Value("${zookeeper.address}")
    public void setAddress (String address) {
        HBaseConfig.address = address;
    }
}
