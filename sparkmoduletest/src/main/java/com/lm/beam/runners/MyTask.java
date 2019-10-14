package com.lm.beam.runners;

/**
 * @Author: limeng
 * @Date: 2019/10/8 18:58
 */
public class MyTask implements Runnable {
    @Override
    public void run() {

    }

    public static void main(String[] args) {
        int i=2047;
        int i2 = 2 << 10;
        int i1 = i & (i2-1);

        int i3 = 8 << 1;
        System.out.println(i3);
    }
}
