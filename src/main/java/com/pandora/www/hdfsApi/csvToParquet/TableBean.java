package com.pandora.www.hdfsApi.csvToParquet;

public class TableBean {
    String id;
    String name;

    public TableBean() {
    }

    public TableBean(String id, String name) {
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
