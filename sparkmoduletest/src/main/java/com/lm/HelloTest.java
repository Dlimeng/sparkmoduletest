package com.lm;

/**
 * @Classname HelloTest
 * @Description TODO
 * @Date 2020/5/10 14:05
 * @Created by limeng
 */
public class HelloTest {
    public static void main(String[] args) {
        String s="hello";
        if(false && false){
            System.out.println("cc");
        }

        String[] test ={"1"};
        try {
            String s1 = test[2];
            String s2 = test[3];

            System.out.println("s1:"+s1);

            System.out.println("s2:"+s2);
        }catch (Exception e){

        }

        System.out.println("cccc");
    }
}
