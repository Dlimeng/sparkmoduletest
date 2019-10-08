package com.lm.beam.neo4j;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/9/20 19:12
 */
@Data
public class Neo4jObject  implements Serializable {
    private Map<String,Object> parMap;
    private Object[] Objects;

    public Object[] getObjectValue(){
        List<Object> list=new ArrayList<>();
        parMap.forEach((k,v)->{
            list.add(k);
            list.add(v);
        });

        return list.toArray();
    }
}
