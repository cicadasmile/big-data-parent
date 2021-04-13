package com.flink.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        // 读取文件
        // readFile () ;
        readPort ();
    }

    public static void readPort () throws Exception {
        // 1、执行环境创建
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2、读取Socket数据端口
        DataStreamSource<String> inputStream = environment.socketTextStream("hop01", 5566);

        // 3、数据读取个切割方式
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = inputStream.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>()
        {
            @Override
            public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) {
                String[] wordArr = input.split(",");
                for (String word : wordArr) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(0).sum(1);

        // 4、打印分析结果
        resultDataStream.print();

        // 5、环境启动
        environment.execute();
    }

    public static void readFile () throws Exception {
        // 1、执行环境创建
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // 2、读取数据文件
        String filePath = "/var/flink/test/word.txt" ;
        DataSet<String> inputFile = environment.readTextFile(filePath);

        // 3、分组并求和
        DataSet<Tuple2<String, Integer>> wordDataSet = inputFile.flatMap(new WordFlatMapFunction(
        )).groupBy(0).sum(1);

        // 4、打印处理结果
        wordDataSet.print();
    }

    // 数据读取个切割方式
    static class WordFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String input, Collector<Tuple2<String, Integer>> collector){
            String[] wordArr = input.split(",");
            for (String word : wordArr) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
