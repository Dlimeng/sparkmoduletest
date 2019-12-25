import org.junit.Test;

/**
 * @Author: limeng
 * @Date: 2019/7/24 10:54
 */
public class DemoTest {
    static boolean boolValue;
    public static void main(String[] args) {
        boolValue = true; // 将这个true替换为2或者3，再看看打印结果
        if (boolValue) System.out.println("Hello, Java!");
        if (boolValue == true) System.out.println("Hello, JVM!");

    }

}
