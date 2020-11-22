package com.case01.map.reduce.reducer;

import com.case01.map.reduce.entity.AddEntity;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AddReducer extends Reducer<Text, AddEntity, Text, AddEntity> {

    @Override
    protected void reduce(Text key, Iterable<AddEntity> values, Context context)
            throws IOException, InterruptedException {

        long addNum01Sum = 0;
        long addNum02Sum = 0;

        // 处理Key相同
        for (AddEntity addEntity : values) {
            addNum01Sum += addEntity.getAddNum01();
            addNum02Sum += addEntity.getAddNum02();
        }

        // 最终输出
        AddEntity addRes = new AddEntity(addNum01Sum, addNum02Sum);
        context.write(key, addRes);
    }

}
