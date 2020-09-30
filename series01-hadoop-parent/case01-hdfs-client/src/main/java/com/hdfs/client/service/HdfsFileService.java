package com.hdfs.client.service;

public interface HdfsFileService {

    // 创建文件夹
    void mkdirs(String path) throws Exception ;

    // 文件判断
    void isFile(String path) throws Exception ;

    // 修改文件名
    void reName(String oldFile, String newFile) throws Exception ;

    // 文件详情
    void fileDetail(String path) throws Exception ;

    // 文件上传
    void copyFromLocalFile(String local, String path) throws Exception ;

    // 拷贝到本地：下载
    void copyToLocalFile(String src, String dst) throws Exception ;

    // 删除文件夹
    void delete(String path) throws Exception ;

    // IO流上传
    void ioUpload(String path, String local) throws Exception ;

    // IO流下载
    void ioDown(String path, String local) throws Exception ;

    // 分块下载
    void blockDown(String path, String local1, String local2) throws Exception ;
}
