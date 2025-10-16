package org.muralis.datahose.dto;

import java.io.Serializable;

public class Visit implements Serializable {
    private static final long serialVersionUID = 1L;

    public String name;
    public String date;

    public Visit() {}

    public Visit(String name, String date) {
        this.name = name;
        this.date = date;
    }
}
