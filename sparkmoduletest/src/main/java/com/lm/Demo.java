package com.lm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @Classname Demo
 * @Description TODO
 * @Date 2020/2/6 23:50
 * @Created by limeng
 */
@Data
public class Demo {
    private String id;
    private Double age;

    public Demo() {
    }

    public Demo(String id, Double age) {
        this.id = id;
        this.age = age;
    }

    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket1 = new ServerSocket(0);
        ServerSocket serverSocket2 = new ServerSocket(0);

        System.out.println(serverSocket1.getLocalPort());
        System.out.println(serverSocket2.getLocalPort());
    }

    public static class ActiveAgencyNumVO{
        private String id;
        private String value;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    @Test
    public void regTest(){

    }
}
