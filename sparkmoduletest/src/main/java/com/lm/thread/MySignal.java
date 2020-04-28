package com.lm.thread;

/**
 * @Classname MySignal
 * @Description TODO
 * @Date 2020/4/28 9:48
 * @Created by limeng
 * 持有信号对象
 */
public class MySignal {
    private boolean hasDataToProcess;

    public synchronized void setHasDataToProcess(boolean hasData){
        this.hasDataToProcess = hasData;
    }

    public synchronized  boolean hasDataToProcess(){
        return this.hasDataToProcess;
    }
}
