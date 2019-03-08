package com.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;


/**
 * version:
 * Created by Guoyn on 2019/3/7.
 */
public class HDFSFileUtils {


    private static FileSystem fs;
    private static Configuration conf;

    static {
        try {
            conf = new Configuration();
            //hdfs://adhnamenode
            fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 实现的命令: hadoop fs -ls hdfsPath
     *
     * @param hdfsPath 写HDFS路径 eg: hdfs://localhost:9000/18565_dailiy_res
     * @Return void
     * @Version: 1.0
     * Create by guoyuannan on 2019/3/7
     */
    public static void getHdfsAllFiles(String hdfsPath) {

        try {
            FileStatus[] status = fs.listStatus(new Path(hdfsPath));
            Path[] paths = FileUtil.stat2Paths(status);

            for (Path p : paths) {
                System.out.println(p);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * 实现的命令: hadoop fs -getmerge hdfsPath localPath
     *
     * @param hdfsPath
     * @param localFile
     * @Return boolean
     * @Version: 1.0
     * Create by guoyuannan on 2019/3/7
     */
    public static boolean mergeHdfsToLocalFile(String hdfsPath, String localFile) {


        try {
            Path inputPath = new Path(hdfsPath);
            Path outPath = new Path(localFile);

            FileSystem localFS = FileSystem.getLocal(conf);

            //如果已经存在本地文件，则删除
            if (localFS.exists(outPath)) {
                localFS.delete(outPath, true);
            }

            //TODO 已过时，还没有找到替代的方法，待查
            // Use copy merge to combine all of the input files
            return org.apache.hadoop.fs.FileUtil.copyMerge(fs, inputPath, localFS,
                    outPath, false, conf, null);


        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }


    /**
     * hadoop fs -get hdfsDir localDir
     *
     * @param hdfsDir
     * @param localDir
     * @Return void
     * @Version: 1.0
     * Create by guoyuannan on 2019/3/7
     */
    public static void copyHdfsDirToLocal(String hdfsDir, String localDir) {

        try {
            Path inputPath = new Path(hdfsDir);
            Path outPath = new Path(localDir);

            FileSystem localFS = FileSystem.getLocal(conf);

            //如果已经存在本地文件，则删除它
            if (localFS.exists(outPath)) {
                localFS.delete(outPath, true);
            }

            fs.copyToLocalFile(false, inputPath, outPath);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 用于判断MR最后存储文件是否成功
     *
     * @param hadoopFilePath
     * @Return boolean
     * @Version: 1.0
     * Create by guoyuannan on 2019/3/7
     */
    public static boolean CheckHaddopSuccess(String hadoopFilePath) {
        boolean ret = false;
        try {

            String filePath = hadoopFilePath + "/_SUCCESS";
            Path findf = new Path(filePath);
            System.out.println("###check success file ->" + filePath);
            ret = fs.exists(findf);
            System.out.println("###check success ret  ->" + ret);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return ret;
        }
    }


    /**
     * 判断hdfs是否存在文件
     *
     * @param hdfsPath
     * @Return: boolean
     * @Version: 1.0
     * Create by guoyuannan on 2019/3/7
     */
    public static boolean exists(String hdfsPath) {
        boolean b = false;
        try {

            b = fs.exists(new Path(hdfsPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return b;
    }


    public static void main(String[] args) {

        String hdfsPath = "/18565_dailiy_res";
        String localPath = "/Users/guoyuannan/tmp/18565_test_bb";
        getHdfsAllFiles(hdfsPath);
        System.out.println(CheckHaddopSuccess(hdfsPath));
        mergeHdfsToLocalFile(hdfsPath, localPath);
        copyHdfsDirToLocal(hdfsPath, localPath+"_copy");



    }
}
