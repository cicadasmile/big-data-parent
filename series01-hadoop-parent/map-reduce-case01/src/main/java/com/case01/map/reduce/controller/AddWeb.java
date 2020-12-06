package com.case01.map.reduce.controller;

import com.case01.map.reduce.config.MapReduceConfig;
import com.case01.map.reduce.entity.AddEntity;
import com.case01.map.reduce.mapper.AddMapper;
import com.case01.map.reduce.reducer.AddReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.IOException;

@RestController
public class AddWeb {

    @Resource
    private MapReduceConfig mapReduceConfig ;

    @GetMapping("/getAdd")
    public String getAdd () throws IOException, ClassNotFoundException, InterruptedException {
        // 声明配置
        Configuration addConfig = new Configuration();
        addConfig.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        addConfig.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        Job job = Job.getInstance(addConfig);

        // Job执行作业 输入路径
        FileInputFormat.addInputPath(job, new Path("/Users/Shared/source/addSource.csv"));
        // Job执行作业 输出路径
        FileOutputFormat.setOutputPath(job, new Path("/Users/Shared/addRes"));

        // 自定义 Mapper和Reducer 两个阶段的任务处理类
        job.setMapperClass(AddMapper.class);
        job.setReducerClass(AddReducer.class);

        // 指定mapper输出数据的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AddEntity.class);

        // 设置输出结果的Key和Value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AddEntity.class);

        // 执行Job直到完成
        job.waitForCompletion(true);
        return "success" ;
    }
}
