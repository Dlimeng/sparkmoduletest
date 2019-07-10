package com.lm.beam.sql.model;

import com.google.auto.value.AutoValue;

import java.io.Serializable;

/**
 * @Author: limeng
 * @Date: 2019/7/10 15:30
 */
@AutoValue
public class TestRow  implements Serializable, Comparable<TestRow> {

    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int compareTo(TestRow other) {
        return name.compareTo(other.name);
    }
}
