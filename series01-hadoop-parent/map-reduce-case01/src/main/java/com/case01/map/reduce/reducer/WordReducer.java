package com.case01.map.reduce.reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    int sum ;
    IntWritable value = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context)
                        throws IOException, InterruptedException {
        // 1、累加求和统计
        sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }
        // 2、输出结果
        value.set(sum);
        context.write(key,value);
    }
}
