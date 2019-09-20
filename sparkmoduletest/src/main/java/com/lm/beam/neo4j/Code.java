package com.lm.beam.neo4j;

import lombok.Data;
/**
 * @Author: limeng
 * @Date: 2019/9/19 15:55
 */
@Data
public class Code {
    private String id;
    private String node;
    private String relation;
    private String property;
    private String label;

    private String nodeFromId;
    private String nodeFromLabel;
    private String nodeToId;
    private String nodeToLabel;

    private String where;
    private String update;
    private String result;


}
