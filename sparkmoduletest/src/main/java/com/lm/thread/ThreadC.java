package com.lm.thread;

/**
 * @Classname ThreadC
 * @Description TODO
 * @Date 2020/4/28 12:07
 * @Created by limeng
 */
public class ThreadC extends Thread {
    MonitorObject mySignal;
    ThreadD threadD;

    public ThreadC(MonitorObject mySignal,ThreadD threadD){
        this.mySignal = mySignal;
        this.threadD = threadD;
    }

    @Override
    public void run() {
        synchronized (mySignal){

            try {
                mySignal.wait();
            }catch (Exception e){
                e.printStackTrace();
            }

            System.out.println("线程B计算结果："+threadD.count);

        }
    }


    public static void main(String[] args) {
        MonitorObject monitorObject = new MonitorObject();
        ThreadD threadD = new ThreadD(monitorObject);
        ThreadC threadC = new ThreadC(monitorObject, threadD);

        threadC.start();
        threadD.start();
    }
}
