package com.baidu.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HadoopHdfs {

    /**
     * 客户端 会自动加载resource 的hdfs文件
     * DataNode节点信息存放在slaves文件中
     */
    private Configuration configuration = null;
    /**
     * abstract class parent 接收 Configuration返回的子类实现
     */
    private FileSystem fileSystem = null;

    @Before
    public void beforeConn() throws IOException, InterruptedException {
        configuration = new Configuration(true);
        //自定义用户和 mycluster
        fileSystem = FileSystem.get(URI.create("hdfs://mycluster/"),configuration,"god");
    }

    @Test
    public void test() throws IOException {
        Path path = new Path("/mashibing1");
        if(fileSystem.exists(path)){
            fileSystem.delete(path);
        }
        fileSystem.mkdirs(path);
    }
    @Test
    public void uploadFile() throws IOException {
        //读文件到本地内存
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));
        //创建hadoop文件路径
        Path path = new Path("/msb/out.txt");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
        IOUtils.copyBytes(bufferedInputStream,fsDataOutputStream,configuration,true);
    }

    @Test
    public void blocks() throws IOException {

        Path path = new Path("/user/god/data.txt");
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation blockLocation :fileBlockLocations) {
            System.out.println(blockLocation);
        }

        FSDataInputStream in = fileSystem.open(path);
        //指定位置读文件
        in.seek(1040319);
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());



    }

    @After
    public void close() throws IOException {
       fileSystem.close();
    }




}
