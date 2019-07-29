package com.lm.beam.sql;

/**
 * @Author: limeng
 * @Date: 2019/7/25 21:23
 */
public class DemoTest {
    public static void main(String[] args) {
        String str= "sdasdasdasdasd；";
        boolean matches = str.matches("\\;|\\；");
        System.out.println(matches);
        //str.
        //int i = str.lastIndexOf(str.length() - 1);
        String[] split = str.split(";|；");
        if(split != null){
            System.out.println();
        }
        System.out.println(split);
        //boolean b = str.contains("\\;|\\；");
       // System.out.println(b);
    }
}
