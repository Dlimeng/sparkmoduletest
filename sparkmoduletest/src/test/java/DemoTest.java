import org.junit.Test;

import java.util.LinkedList;

/**
 * @Author: limeng
 * @Date: 2019/7/24 10:54
 */
public class DemoTest {
    static boolean boolValue;
    public static void main(String[] args) {
//        boolValue = true; // 将这个true替换为2或者3，再看看打印结果
//        if (boolValue) System.out.println("Hello, Java!");
//        if (boolValue == true) System.out.println("Hello, JVM!");


//        LinkedList<String> strings = new LinkedList<>();
//
//        strings.add(0,"test1");
//        strings.add(0,"test2");
//        strings.add(0,"test3");
//
//        System.out.println(strings);

        System.out.println("main start");
        Thread thread = new Thread(new TestThread());
        thread.start();

        System.out.println("main end");
    }


    static class TestThread implements Runnable{

        @Override
        public void run() {
            System.out.println("test start");
            try {
                new Object().wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("test end");
        }
    }

}
