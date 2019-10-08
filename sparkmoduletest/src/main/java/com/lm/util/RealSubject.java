package com.lm.util;

/**
 * @Author: limeng
 * @Date: 2019/9/24 14:45
 */
public class RealSubject implements Subject  {
    @Override
    public void doSomething() {
        System.out.println("test1");
    }

    public static void main(String[] args) {
        RealSubject proxy = new JDKDynamicProxy(new RealSubject()).getProxy();
        proxy.doSomething();
    }
}
