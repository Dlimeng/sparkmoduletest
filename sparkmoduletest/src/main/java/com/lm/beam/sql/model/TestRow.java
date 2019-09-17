package com.lm.beam.sql.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author: limeng
 * @Date: 2019/7/10 15:30
 */

public class TestRow  implements Serializable, Comparable<TestRow> {

    private String id;
    private String name;

    public TestRow() {
    }

    public TestRow(String id, String name) {
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

    @Override
    public int compareTo(TestRow other) {
        return name.compareTo(other.name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestRow tr = (TestRow) o;
        return id == tr.id
                && Objects.equals(name, tr.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return "TestRow{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
