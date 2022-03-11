package org.ssaikia.realm.spark;

import java.io.Serializable;

public class Customer implements Serializable {
    private String name;
    private String place;

    public Customer(String name, String place) {
        super();
        this.name = name;
        this.place = place;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPlace() {
        return place;
    }

    public void setPlace(String place) {
        this.place = place;
    }
}
