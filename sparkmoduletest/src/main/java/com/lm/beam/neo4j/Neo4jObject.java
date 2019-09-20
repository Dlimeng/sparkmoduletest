package com.lm.beam.neo4j;

import lombok.Data;

import java.io.Serializable;

/**
 * @Author: limeng
 * @Date: 2019/9/20 19:12
 */
@Data
public class Neo4jObject  implements Serializable {
    private Object[] objects;
}
