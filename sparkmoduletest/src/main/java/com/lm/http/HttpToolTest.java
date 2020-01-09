package com.lm.http;

import com.lm.util.HttpTool;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Classname HttpToolTest
 * @Description TODO
 * @Date 2019/12/31 18:17
 * @Created by limeng
 */
@RunWith(JUnit4.class)
public class HttpToolTest {
    @Test
    public void testGet(){

        Map<Object, Object> param = new HashMap<>();

        param.put("dataSourceId","24dacc64234141ffb3cd9ccc5b147b07");
        param.put("token","f0df7414507bcb57e07e18555821228a");
        try {
            Object result = HttpTool.get("http://localhost:8080/api/swap/data/query/id", param, Map.class);
            /**
             * {code=0, data={dataName=mysql测试, dataType=1, dataUrl=jdbc:mysql://192.168.200.115:3306/kd_test?useSSL=false, dataPort=null, dataUsername=root, dataPassword=root, desc=null, status=0, ctime=2019-12-31T08:14:56.000+0000, connectedStatus=200, connectedCtime=2019-12-31T08:14:56.000+0000, createId=6e59033c2be847778acf4c9c88a1f600, dataVersion=null, dbName=kd_test, fileBasePath=null, mid=24dacc64234141ffb3cd9ccc5b147b07}, message=success}
             */
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPost(){
        try {
            Map<Object, Object> param = new HashMap<>();

            param.put("dataType","1");
            param.put("token","f0df7414507bcb57e07e18555821228a");
            param.put("dataUsername","root");
            param.put("dataPassword","root");
            param.put("dataUrls","jdbc:mysql://192.168.200.115:3306/kd_swap?useSSL=false");
            Object result = HttpTool.post("http://localhost:8080/api/swap/data/test/connection", param, Map.class);
            /**
             * {code=0, data={status=true}, message=success}
             */
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
