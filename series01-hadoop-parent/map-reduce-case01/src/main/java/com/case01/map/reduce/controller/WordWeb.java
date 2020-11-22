package com.case01.map.reduce.controller;

import java.io.IOException;

import com.case01.map.reduce.config.MapReduceConfig;
import com.case01.map.reduce.mapper.WordMapper;
import com.case01.map.reduce.reducer.WordReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
public class WordWeb {

    @Resource
    private MapReduceConfig mapReduceConfig ;

    @GetMapping("/getWord")
    public String getWord () throws IOException, ClassNotFoundException, InterruptedException {
        // 声明配置
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        hadoopConfig.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        Job job = Job.getInstance(hadoopConfig);

        // Job执行作业 输入路径
        FileInputFormat.addInputPath(job, new Path(mapReduceConfig.getInputPath()));
        // Job执行作业 输出路径
        FileOutputFormat.setOutputPath(job, new Path(mapReduceConfig.getOutputPath()));

        // 自定义 Mapper和Reducer 两个阶段的任务处理类
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        // 设置输出结果的Key和Value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //执行Job直到完成
        job.waitForCompletion(true);
        return "success" ;
    }
}
