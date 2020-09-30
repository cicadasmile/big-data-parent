package com.hdfs.client.service;

import com.hdfs.client.config.HdfsConfig;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;

@Service
public class HdfsFileServiceImpl implements HdfsFileService {

    @Resource
    private HdfsConfig hdfsConfig ;

    @Override
    public void mkdirs(String path) throws Exception {
        // 1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfsConfig.getNameNode()),
                                               configuration, "root");
        // 2、创建目录
        fileSystem.mkdirs(new Path(path));
        // 3、关闭资源
        fileSystem.close();
    }

    @Override
    public void isFile(String path) throws Exception {
        // 1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfsConfig.getNameNode()),
                                               configuration, "root");
        // 2、判断文件和文件夹
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件:"+fileStatus.getPath().getName());
            }else {
                System.out.println("文件夹:"+fileStatus.getPath().getName());
            }
        }
        // 3、关闭资源
        fileSystem.close();
    }

    @Override
    public void reName(String oldFile, String newFile) throws Exception {
        // 1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfsConfig.getNameNode()),
                configuration, "root");
        // 2、修改文件名
        fileSystem.rename(new Path(oldFile), new Path(newFile));
        // 3、关闭资源
        fileSystem.close();
    }

    @Override
    public void fileDetail(String path) throws Exception {
        // 1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfsConfig.getNameNode()),
                                               configuration, "root");
        // 2、读取文件详情
        RemoteIterator<LocatedFileStatus> listFiles =
                                    fileSystem.listFiles(new Path(path), true);
        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            System.out.println("文件名："+status.getPath().getName());
            System.out.println("文件长度："+status.getLen());
            System.out.println("文件权限："+status.getPermission());
            System.out.println("所属分组："+status.getGroup());
            // 存储块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.print(host+";");
                }
            }
            System.out.println("==============Next==============");
        }
        // 3、关闭资源
        fileSystem.close();
    }

    @Override
    public void copyFromLocalFile(String local, String path) throws Exception {
        // 1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfsConfig.getNameNode()),
                                configuration, "root");
        // 2、执行上传操作
        fileSystem.copyFromLocalFile(new Path(local), new Path(path));
        // 3、关闭资源
        fileSystem.close();
    }

    @Override
    public void copyToLocalFile(String src,String dst) throws Exception {
        // 1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfsConfig.getNameNode()),
                                               configuration, "root");
        // 2、执行下载操作
        // src 服务器文件路径 ; dst 文件下载到的路径
        fileSystem.copyToLocalFile(false, new Path(src), new Path(dst), true);
        // 3、关闭资源
        fileSystem.close();
    }

    @Override
    public void delete(String path) throws Exception {
        // 1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfsConfig.getNameNode()),
                                configuration, "root");
        // 2、删除文件或目录 是否递归
        fileSystem.delete(new Path(path), true);
        // 3、关闭资源
        fileSystem.close();
    }

    @Override
    public void ioUpload(String path, String local) throws Exception {
        // 1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfsConfig.getNameNode()),
                                configuration, "root");
        // 2、输入输出流
        FileInputStream fis = new FileInputStream(new File(local));
        FSDataOutputStream fos = fileSystem.create(new Path(path));
        // 3、流对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 4、关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystem.close();
    }

    @Override
    public void ioDown(String path, String local) throws Exception {
        // 1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfsConfig.getNameNode()),
                configuration, "root");
        // 2、输入输出流
        FSDataInputStream fis = fileSystem.open(new Path(path));
        FileOutputStream fos = new FileOutputStream(new File(local));
        // 3、流对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 4、关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystem.close();
    }

    @Override
    public void blockDown(String path,String local1,String local2) throws Exception {
        readFileSeek01(path,local1);
        readFileSeek02(path,local2);
    }

    private void readFileSeek01(String path,String local) throws Exception {
        // 1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfsConfig.getNameNode()),
                                configuration, "root");
        // 2、输入输出流
        FSDataInputStream fis = fileSystem.open(new Path(path));
        FileOutputStream fos = new FileOutputStream(new File(local));
        // 3、部分拷贝
        byte[] buf = new byte[1024];
        for(int i =0 ; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }
        // 4、关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystem.close();
    }

    private void readFileSeek02(String path,String local) throws Exception {
        // 1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfsConfig.getNameNode()),
                configuration, "root");
        // 2、输入输出流
        FSDataInputStream fis = fileSystem.open(new Path(path));
        // 定位输入数据位置
        fis.seek(1024*1024*128);
        FileOutputStream fos = new FileOutputStream(new File(local));
        // 3、流拷贝
        IOUtils.copyBytes(fis, fos, configuration);
        // 4、关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystem.close();
    }

}
