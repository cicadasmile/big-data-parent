package com.case01.map.reduce.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AddEntity implements Writable {

    private long addNum01;
    private long addNum02;
    private long resNum;

    // 构造方法
    public AddEntity() {
        super();
    }
    public AddEntity(long addNum01, long addNum02) {
        super();
        this.addNum01 = addNum01;
        this.addNum02 = addNum02;
        this.resNum = addNum01 + addNum02;
    }

    // 序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(addNum01);
        dataOutput.writeLong(addNum02);
        dataOutput.writeLong(resNum);
    }
    // 反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 注意：反序列化顺序和写序列化顺序一致
        this.addNum01  = dataInput.readLong();
        this.addNum02 = dataInput.readLong();
        this.resNum = dataInput.readLong();
    }
    // 打印内容
    @Override
    public String toString() {
        return addNum01 + "," + addNum02 + "," + resNum;
    }

    public long getAddNum01() {
        return addNum01;
    }

    public void setAddNum01(long addNum01) {
        this.addNum01 = addNum01;
    }

    public long getAddNum02() {
        return addNum02;
    }

    public void setAddNum02(long addNum02) {
        this.addNum02 = addNum02;
    }

    public long getResNum() {
        return resNum;
    }

    public void setResNum(long resNum) {
        this.resNum = resNum;
    }
}
