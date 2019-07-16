package com.lm.beam.sql.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

/**
 * @Author: limeng
 * @Date: 2019/7/15 14:19
 */
//@DefaultCoder(AvroCoder.class)
public class TestRow2 implements Serializable {

    private static final long serialVersionUID = 565188219956165887L;

    private String id;
    private String name;

    public TestRow2() {
    }

    public TestRow2(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
