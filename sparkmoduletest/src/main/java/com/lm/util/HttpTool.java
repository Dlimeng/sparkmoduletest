package com.lm.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.*;

/**
 * @Classname HttpTool
 * @Description TODO
 * @Date 2019/12/31 18:15
 * @Created by limeng
 */
public class HttpTool {
    private static Logger logger = LoggerFactory.getLogger(HttpTool.class);
    private static HttpClient httpclient = HttpClientBuilder.create().build();

    public static Object get(String url, Map<Object, Object> param, Class resultClass) throws IOException {

        return doGet(url, param, resultClass);
    }

    private static Object doGet(String url, Map<Object, Object> param, Class resultClass) throws IOException {
        StringBuffer sb = new StringBuffer();
        if (param != null) {
            boolean first = false;
            for (Map.Entry<Object, Object> entry : param.entrySet()) {
                if(first){
                    sb.append("&");
                }else{
                    first = true;
                }
                sb.append(entry.getKey()).append("=").append(entry.getValue());
            }
        }


        HttpResponse response;
        if (sb.length() > 0) {
            HttpGet httpGet = new HttpGet(url + "?" + sb.toString());
            response = httpclient.execute(httpGet);

        } else {
            HttpGet httpGet = new HttpGet(url);
            response = httpclient.execute(httpGet);
        }

        if (response != null) {
            byte[] bytes = EntityUtils.toByteArray(response.getEntity());
            Object result = JsonConverter.read(resultClass, bytes);
            return result;
        }

        throw new RuntimeException();
    }

    public static Object post(String url, Object postParam, Class resultClass) throws IOException {
        Map<String, String> param = (Map<String, String>)getMapParam(postParam);

        return doPost(url, resultClass, param);
    }

    private static Object doPost(String url, Class resultClass, Map<String, String> param) throws IOException {
        HttpPost httpPost = new HttpPost(url);
        List<NameValuePair> nvps = new ArrayList<NameValuePair>();


        if (param != null) {
            for (Map.Entry<String, String> entry : param.entrySet()) {
                nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
        }
        httpPost.setEntity(new UrlEncodedFormEntity(nvps, "UTF-8"));
        HttpResponse response = httpclient.execute(httpPost);
        if (response != null) {
            byte[] bytes = EntityUtils.toByteArray(response.getEntity());
            Object result = JsonConverter.read(resultClass, bytes);
            return result;
        }

        throw new RuntimeException();
    }

    public static Object post(String url, Map<String, String> param, Class resultClass) throws IOException {
        return doPost(url, resultClass, param);
    }

    public static Map<?, ?> getMapParam(Object postParam) {
        Map param = null;
        try {
            param = JSONObject.parseObject(JSONObject.toJSONString(postParam), Map.class);
        } catch (Exception e) {
            logger.error("bean trans map error", e);
            throw new RuntimeException("bean trans map error", e);
        }
        return param;
    }


    public static String getIpAddr(HttpServletRequest request) {
        String ip = request.getHeader("x-forwarded-for");
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknow".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        return ip;
    }

    /**
     * 获取局域网IP
     *
     * @return
     * @throws Exception
     */
    public static InetAddress getLocalHostLANAddress() {
        try {
            InetAddress candidateAddress = null;
            // 遍历所有的网络接口
            for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements(); ) {
                NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
                // 在所有的接口下再遍历IP
                for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements(); ) {
                    InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
                    if (!inetAddr.isLoopbackAddress()) {// 排除loopback类型地址
                        if (inetAddr.isSiteLocalAddress()) {
                            // 如果是site-local地址，就是它了
                            return inetAddr;
                        } else if (candidateAddress == null) {
                            // site-local类型的地址未被发现，先记录候选地址
                            candidateAddress = inetAddr;
                        }
                    }
                }
            }
            if (candidateAddress != null) {
                return candidateAddress;
            }
            // 如果没有发现 non-loopback地址.只能用最次选的方案
            return InetAddress.getLocalHost();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
