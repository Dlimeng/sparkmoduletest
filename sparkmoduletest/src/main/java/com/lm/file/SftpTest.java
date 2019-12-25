package com.lm.file;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.Properties;
import java.util.Vector;

/**
 * @Classname SftpTest
 * @Description TODO
 * @Date 2019/12/25 17:07
 * @Created by limeng
 */
public class SftpTest {
    private ChannelSftp channel;
    private Session session;
    private String sftpPath;


    public SftpTest() {
        this.connectServer("192.168.1.150",22,"sftp-limeng","Limeng_321","/data");
    }

    public void connectServer(String ftpHost, int ftpPort, String ftpUserName, String ftpPassword, String sftpPath){
        try {
            this.sftpPath = sftpPath;
            final JSch jSch = new JSch();
            session = jSch.getSession(ftpUserName,ftpHost,ftpPort);
            if(ftpPassword != null){
                session.setPassword(ftpPassword);
            }
            Properties properties = new Properties();
            properties.put("StrictHostKeyChecking", "no");
            session.setConfig(properties);

            //设置timeout
            session.setTimeout(60000);
            session.connect();
            //通过Session建立连接
            //打开通道
            channel = (ChannelSftp) session.openChannel("sftp");
            //建立sftp通道连接
            channel.connect();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public void closeChannel() {
        try {
            if(channel != null){
                channel.disconnect();
            }
            if(session != null){
                session.disconnect();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     *
     * @param localFile
     * @param remoteFile
     */
    public void upload(String localFile,String remoteFile){

        try {
            remoteFile = sftpPath + remoteFile;
            channel.put(localFile,remoteFile, ChannelSftp.OVERWRITE);
            channel.quit();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     *
     * @param basePath 服务基础路径
     * @param directory 上传到该目录
     * @param sftpFileName sftp文件名
     * @param input 输入流
     */
    public void upload(String basePath, String directory, String sftpFileName, InputStream input){
        try {
            channel.cd(basePath);
            channel.cd(directory);
        }catch (Exception e) {
            String[] dirs = directory.split("/");
            String tempPath = basePath;
            for (String dir : dirs) {
                if (null == dir || "".equals(dir)) continue;
                tempPath += "/" + dir;
                try {
                    channel.cd(tempPath);
                } catch (SftpException ex) {
                    try {
                        channel.mkdir(tempPath);
                        channel.cd(tempPath);
                    } catch (SftpException e2) {
                        e2.printStackTrace();
                    }
                }
            }
        }

        try {
            channel.put(input, sftpFileName);  //上传文件
        } catch (SftpException e) {
            e.printStackTrace();
        }
    }


    /**
     * 下载文件。
     * @param directory 下载目录
     * @param downloadFile 下载的文件
     * @param saveFile 存在本地的路径
     */
    public void download(String directory, String downloadFile, String saveFile) throws SftpException, FileNotFoundException {
        if (directory != null && !"".equals(directory)) {
            channel.cd(directory);
        }
        File file = new File(saveFile);
        channel.get(downloadFile, new FileOutputStream(file));
        channel.quit();
    }


    /**
     * 下载文件
     * @param directory 下载目录
     * @param downloadFile 下载的文件名
     * @return 字节数组
     */
    public byte[] download(String directory, String downloadFile) throws SftpException, IOException {
        if (directory != null && !"".equals(directory)) {
            channel.cd(directory);
        }
        InputStream is = channel.get(downloadFile);

        byte[] fileData = IOUtils.toByteArray(is);
        channel.quit();
        return fileData;
    }

    /**
     * 查看目录
     * @param directory
     */
    public void listFiles(String directory){
        try {
            Vector ls = channel.ls(directory);
            ls.forEach(f->{
                System.out.println(f.toString());
            });
        } catch (SftpException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除
     * @param directory 删除文件目录
     * @param deleteFile 删除文件
     */
    public void delete(String directory,String deleteFile){
        try {
            channel.cd(directory);
            channel.rm(deleteFile);
        } catch (SftpException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        SftpTest sftpTest = new SftpTest();
        sftpTest.upload("D:/beam.txt","/beam.txt");
        ///data/sftp-test/limeng
       // sftpTest.listFiles("/data/sftp-test/limeng");
    }

}
