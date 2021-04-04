package com.spark.basic;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@RestController
public class WordWeb implements Serializable {

    @GetMapping("/word/web")
    public String getWeb (){
        // 1、创建Spark的配置对象
        SparkConf sparkConf = new SparkConf().setAppName("LocalCount")
                                             .setMaster("local[*]");

        // 2、创建SparkContext对象
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");

        // 3、读取测试文件
        JavaRDD lineRdd = sc.textFile("/var/spark/test/word.txt");

        // 4、行内容进行切分
        JavaRDD wordsRdd = lineRdd.flatMap(new FlatMapFunction() {
            @Override
            public Iterator call(Object obj) throws Exception {
                String value = String.valueOf(obj);
                String[] words = value.split(",");
                return Arrays.asList(words).iterator();
            }
        });

        // 5、切分的单词进行标注
        JavaPairRDD wordAndOneRdd = wordsRdd.mapToPair(new PairFunction() {
            @Override
            public Tuple2 call(Object obj) throws Exception {
                //将单词进行标记：
                return new Tuple2(String.valueOf(obj), 1);
            }
        });

        // 6、统计单词出现次数
        JavaPairRDD wordAndCountRdd = wordAndOneRdd.reduceByKey(new Function2() {
            @Override
            public Object call(Object obj1, Object obj2) throws Exception {
                return Integer.parseInt(obj1.toString()) + Integer.parseInt(obj2.toString());
            }
        });

        // 7、排序
        JavaPairRDD sortedRdd = wordAndCountRdd.sortByKey();
        List<Tuple2> finalResult = sortedRdd.collect();

        // 8、结果打印
        for (Tuple2 tuple2 : finalResult) {
            System.out.println(tuple2._1 + " ===> " + tuple2._2);
        }

        // 9、保存统计结果
        sortedRdd.saveAsTextFile("/var/spark/output");
        sc.stop();
        return "success" ;
    }
}
