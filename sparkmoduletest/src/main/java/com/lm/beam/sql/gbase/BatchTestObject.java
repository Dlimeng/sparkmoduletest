package com.lm.beam.sql.gbase;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

/**
 * @Author: limeng
 * @Date: 2019/10/15 18:49
 */
@Data
public class BatchTestObject implements Serializable {
    private int id;
    private String name;
    private Date createtime;
    private Date  updatetime;
    private String desc;
    private Double money1;
    private BigDecimal money2;
    private String word;
    private int age;

}
