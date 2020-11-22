package com.case01.map.reduce.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text mapKey = new Text();
    IntWritable mapValue = new IntWritable(1);

    @Override
    protected void map (LongWritable key, Text value, Context context)
                        throws IOException, InterruptedException {
        // 1、读取行
        String line = value.toString();
        // 2、行内容切割，根据文件中分隔符
        String[] words = line.split(" ");
        // 3、存储
        for (String word : words) {
            mapKey.set(word);
            context.write(mapKey, mapValue);
        }
    }
}
