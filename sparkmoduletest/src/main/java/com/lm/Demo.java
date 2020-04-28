package com.lm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Classname Demo
 * @Description TODO
 * @Date 2020/2/6 23:50
 * @Created by limeng
 */
@Data
public class Demo {
    private String id;
    private Double age;

    public Demo() {
    }

    public Demo(String id, Double age) {
        this.id = id;
        this.age = age;
    }

    public static void main(String[] args) {
//        Double d=12.1234;
//        BigDecimal bValue  =  new BigDecimal(d);
//         BigDecimal bigDecimal = bValue.setScale(2, BigDecimal.ROUND_HALF_UP);
//        System.out.println(bigDecimal.doubleValue());
//        final Demo demo = new Demo("1",10d);
//
//        final Demo demo2 = new Demo("2",12d);
//
//        final Demo demo3 = new Demo("3",13d);
//
//        List<Demo> list=new ArrayList<>();
//        list.add(demo);
//        list.add(demo2);
//        list.add(demo3);
//
//        final List<Demo> collect = list.stream().sorted(Comparator.comparing(Demo::getAge)).collect(Collectors.toList());
//
//        final List<Demo> collect1 = list.stream().sorted(Comparator.comparing(Demo::getAge).reversed()).collect(Collectors.toList());
//        System.out.println(collect);
//        System.out.println(collect1);

//        long seconds=1581079337607L;
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        final String format = sdf.format(new Date(seconds));
//        System.out.println(format);

//        String str="ccBCccc";
//        String s2="bc";
//        if(str.contains(s2)){
//            System.out.println("true");
//        }
//        if(str.indexOf(s2) != -1){
//            System.out.println("true");
//        }

//        Map<String, Object> map = new LinkedHashMap<>();
//        map.put("k1",102L);
//        map.put("k2",2L);
//        map.put("k3",12L);
//        map.put("k4",32L);
//
//        JSONObject json =  new JSONObject(map);
//
//        String sjson = json.toJSONString();
//        System.out.println(sjson);
//        Map parse = (Map) JSONObject.parse(sjson);
//        System.out.println(parse);
//
//        ActiveAgencyNumVO agency=null;
//
//        List<ActiveAgencyNumVO> list=new ArrayList<>();
//        for (Map.Entry<String, Object> entry : map.entrySet()){
//            agency = new ActiveAgencyNumVO();
//            agency.setId(entry.getKey());
//            agency.setValue(entry.getValue().toString());
//            list.add(agency);
//        }
//        System.out.println(list);



//        List<Demo> list = new ArrayList<>();
//
//        Demo demo = new Demo();
//        demo.setId("id1");
//        demo.setAge(12d);
//
//        Demo demo2 = new Demo();
//        demo2.setId("id2");
//        demo2.setAge(24d);
//
//        list.add(demo);
//        list.add(demo2);
//        list.stream().sorted(Comparator.comparing(Demo::getAge).reversed()).collect(Collectors.toList());
//
//        System.out.println(list);

       // System.out.println(20971520 >> 7);
        //System.out.println(5<<-10);

        String pathString="file://192.68.20.1/tmp/linkis";
        String scheme = null;
        String authority = null;
        int start = 0;
        int colon = pathString.indexOf(58);
        int slash = pathString.indexOf(47);
        if (colon != -1 && (slash == -1 || colon < slash)) {
            scheme = pathString.substring(0, colon);
            start = colon + 1;
        }

        if (pathString.startsWith("//", start) && pathString.length() - start > 2) {
            int nextSlash = pathString.indexOf(47, start + 2);
            int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority = pathString.substring(start + 2, authEnd);
            start = authEnd;
        }

        System.out.println("ccc");

    }

    public static class ActiveAgencyNumVO{
        private String id;
        private String value;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
