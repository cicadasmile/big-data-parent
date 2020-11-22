package com.case01.map.reduce.mapper;

import com.case01.map.reduce.entity.AddEntity;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AddMapper extends Mapper<LongWritable, Text, Text, AddEntity> {

    Text myKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 读取行
        String line = value.toString();

        // 行内容切割
        String[] lineArr = line.split(",");

        // 内容格式处理
        String lineNum = lineArr[0];
        long addNum01 = Long.parseLong(lineArr[1]);
        long addNum02 = Long.parseLong(lineArr[2]);

        myKey.set(lineNum);
        AddEntity myValue = new AddEntity(addNum01,addNum02);

        // 输出
        context.write(myKey, myValue);
    }
}
