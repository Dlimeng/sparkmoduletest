package com.lm.thread;

/**
 * @Classname MonitorObject
 * @Description TODO
 * @Date 2020/4/28 10:51
 * @Created by limeng
 * 任意对象，为监控对象，调用线程必须先获得这个对象锁，
 * 因为线程只有在同步块中才会占用对象的锁，
 * 所以线程必须在同步调用wait(),notify()
 *
 */
public class MonitorObject {


}
