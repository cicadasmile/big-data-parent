package com.hdfs.client.controller;

import com.hdfs.client.service.HdfsFileService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
public class HDFSController {

    @Resource
    private HdfsFileService hdfsFileService ;

    private static final String SUCCESS = "success";
    private static final String FAIL = "fail";

    @GetMapping("/mdDirs")
    public String mdDirs () {
        try {
            hdfsFileService.mkdirs("/hopdir/file0703");
        } catch (Exception e) {
            return FAIL ;
        }
        return SUCCESS ;
    }

    @GetMapping("/copyFromLocalFile")
    public String copyFromLocalFile() {
        try {
            String local = "/Users/Shared/java.txt" ;
            String path = "/hopdir/java.txt" ;
            hdfsFileService.copyFromLocalFile(local,path) ;
        } catch (Exception e) {
            return FAIL ;
        }
        return SUCCESS ;
    }

    @GetMapping("/isFile")
    public String isFile () {
        try {
            hdfsFileService.isFile("/hopdir");
            hdfsFileService.isFile("/hopdir/java.txt");
        } catch (Exception e) {
            return FAIL ;
        }
        return SUCCESS ;
    }

    @GetMapping("/reName")
    public String reName () {
        try {
            String oldFile = "/hopdir/java.txt";
            String newFile = "/hopdir/javaNew.txt";
            hdfsFileService.reName(oldFile,newFile);
        } catch (Exception e) {
            return FAIL ;
        }
        return SUCCESS ;
    }

    @GetMapping("/fileDetail")
    public String fileDetail () {
        try {
            hdfsFileService.fileDetail("/hopdir");
        } catch (Exception e) {
            return FAIL ;
        }
        return SUCCESS ;
    }

    @GetMapping("/copyToLocalFile")
    public String copyToLocalFile() {
        try {
            String src = "/hopdir/javaNew.txt" ;
            String dst = "/Users/Shared/javaNew.txt" ;
            hdfsFileService.copyToLocalFile(src,dst) ;
        } catch (Exception e) {
            return FAIL ;
        }
        return SUCCESS ;
    }

    @GetMapping("/delete")
    public String delete() {
        try {
            String path = "/hopdir/javaNew.txt" ;
            hdfsFileService.delete(path) ;
        } catch (Exception e) {
            return FAIL ;
        }
        return SUCCESS ;
    }

    @GetMapping("/ioUpload")
    public String ioUpload() {
        try {
            String path = "/hopdir/hadoop-2.7.2.zip" ;
            String local = "/Users/Shared/hadoop-2.7.2.zip" ;
            hdfsFileService.ioUpload(path,local) ;
        } catch (Exception e) {
            return FAIL ;
        }
        return SUCCESS ;
    }

    @GetMapping("/ioDown")
    public String ioDown() {
        try {
            String path = "/hopdir/javaNew.txt" ;
            String local = "/Users/Shared/java01.txt" ;
            hdfsFileService.ioDown(path,local) ;
        } catch (Exception e) {
            return FAIL ;
        }
        return SUCCESS ;
    }

    @GetMapping("/blockDown")
    public String blockDown() {
        try {
            String path = "/hopdir/hadoop-2.7.2.zip" ;
            String local1 = "/Users/Shared/hadoop-2.7.2.zip.block1" ;
            String local2 = "/Users/Shared/hadoop-2.7.2.zip.block2" ;
            hdfsFileService.blockDown(path,local1,local2) ;
        } catch (Exception e) {
            return FAIL ;
        }
        return SUCCESS ;
    }
}
