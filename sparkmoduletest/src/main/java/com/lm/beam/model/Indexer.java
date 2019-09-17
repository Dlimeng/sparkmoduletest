package com.lm.beam.model;

import com.lm.beam.common.StoredAsProperty;

/**
 * @Author: limeng
 * @Date: 2019/8/29 15:57
 */
public class Indexer {
    @StoredAsProperty("pattern")
    private String pattern1;
    @StoredAsProperty("sourceName")
    private String sourceName2;
    private String test;

    public String getPattern1() {
        return pattern1;
    }

    public void setPattern1(String pattern1) {
        this.pattern1 = pattern1;
    }

    public String getSourceName2() {
        return sourceName2;
    }

    public void setSourceName2(String sourceName2) {
        this.sourceName2 = sourceName2;
    }

    public String getTest() {
        return test;
    }

    public void setTest(String test) {
        this.test = test;
    }
}
