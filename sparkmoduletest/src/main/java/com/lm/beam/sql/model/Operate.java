package com.lm.beam.sql.model;

/**
 * 操作
 * @Author: limeng
 * @Date: 2019/8/1 13:56
 */
public class Operate {
    private String where;
    private String Select;
    private String delete;

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public String getSelect() {
        return Select;
    }

    public void setSelect(String select) {
        Select = select;
    }

    public String getDelete() {
        return delete;
    }

    public void setDelete(String delete) {
        this.delete = delete;
    }
}
