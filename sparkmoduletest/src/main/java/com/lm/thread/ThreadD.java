package com.lm.thread;

/**
 * @Classname ThreadD
 * @Description TODO
 * @Date 2020/4/28 10:56
 * @Created by limeng
 */
public class ThreadD extends Thread {
    int count;
    MonitorObject mySignal;
    public ThreadD(MonitorObject mySignal){
        this.mySignal = mySignal;
    }

    @Override
    public void run() {
        for (int i = 0; i < 100; i++) {
            count +=1;
        }
        try {
            Thread.sleep(500);
        }catch (Exception e){
            e.printStackTrace();
        }

        synchronized (mySignal){
            mySignal.notify();//
        }
    }
}
