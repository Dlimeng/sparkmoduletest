package com.lm.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;

/**
 * @Classname HDFSFileSystem
 * @Description TODO
 * @Date 2020/12/21 14:05
 * @Created by limeng
 */
public class HDFSFileSystem {
    public static void main(String[] args) {
        String res = args[0];
        long start,end;
        start = System.currentTimeMillis();
        FileSystem fs = init();
        //String res="/tmp/linkis/hdfs/20201215/6e8fa663-ffa8-4d50-9510-f8cf15518dc0.json";
        try {
            FSDataInputStream in = fs.open(new Path(res));
            IOUtils.copyBytes(in, System.out, 4096, false);

        } catch (IOException e) {
            e.printStackTrace();
        }

        end = System.currentTimeMillis();
        System.out.println("\n start time:" + start+ "; end time:" + end+ "; Run Time:" + (end - start) + "(ms)");
    }

    public static FileSystem init(){
        Configuration conf = new Configuration();
        String hadoopConfDir="/etc/hadoop/conf";
        conf.addResource(new Path(Paths.get(hadoopConfDir,"core-site.xml").toAbsolutePath().toFile().getAbsolutePath()));
        conf.addResource(new Path(Paths.get(hadoopConfDir, "hdfs-site.xml").toAbsolutePath().toFile().getAbsolutePath()));
        conf.addResource(new Path(Paths.get(hadoopConfDir, "yarn-site.xml").toAbsolutePath().toFile().getAbsolutePath()));
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            FileSystem fs = UserGroupInformation.createRemoteUser("hdfs").doAs(new PrivilegedExceptionAction<FileSystem>() {
                @Override
                public FileSystem run() throws Exception {
                    return FileSystem.get(conf);
                }
            });
            return fs;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

}
