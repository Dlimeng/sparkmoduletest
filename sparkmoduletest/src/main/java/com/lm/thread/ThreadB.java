package com.lm.thread;

/**
 * @Classname ThreadB
 * @Description TODO
 * @Date 2020/4/28 9:53
 * @Created by limeng
 */
public class ThreadB extends Thread {
    int count;
    MySignal mySignal;

    public ThreadB(MySignal mySignal){
        this.mySignal = mySignal;
    }

    @Override
    public void run() {
        for (int i = 0; i < 100; i++) {
            count = count+1;
        }
        try {
            Thread.sleep(500);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        System.out.println("setHasDataToProcess:");
        mySignal.setHasDataToProcess(true);
    }
}
