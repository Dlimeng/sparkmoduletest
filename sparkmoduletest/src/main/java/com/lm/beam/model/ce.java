package com.lm.beam.model;

import lombok.Data;

/**
 * @Classname ce
 * @Description TODO
 * @Date 2020/2/5 15:41
 * @Created by limeng
 */
@Data
public class ce {
    private String label;
    private String value;
    private String type;
    private String description;
    private Integer sort;
    private String parentId;
    private String id;

    public ce() {
    }

    public ce(String id, String label, String value, String type, String description, Integer sort, String parentId) {
        this.id = id;
        this.label = label;
        this.value = value;
        this.type = type;
        this.description = description;
        this.sort = sort;
        this.parentId = parentId;
    }

    public ce(String label, String value, String type, String description, Integer sort,String parentId) {
        this.label = label;
        this.value = value;
        this.type = type;
        this.description = description;
        this.sort = sort;
        this.parentId = parentId;
    }

    public ce(String label, String value, String type, String description, Integer sort) {
        this.label = label;
        this.value = value;
        this.type = type;
        this.description = description;
        this.sort = sort;
    }

    public ce(String label, String value, String type, String description) {
        this.label = label;
        this.value = value;
        this.type = type;
        this.description=description;
    }
}
