/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ashwin.sec_sort_homicidecity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author ashwi
 */
public class CompositeKey implements WritableComparable<CompositeKey>{

    private String state;
    private String deaths;

    public CompositeKey() {
        super();
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getDeaths() {
        return deaths;
    }

    public void setDeaths(String deaths) {
        this.deaths = deaths;
    }

    public CompositeKey(String state, String deaths) {
        this.state = state;
        this.deaths = deaths;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(state);
        d.writeUTF(deaths);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        state = di.readUTF();
        deaths = di.readUTF();
    }

    @Override
    public int compareTo(CompositeKey o) {
        int result = this.state.compareTo(o.getState());
        if(result==0){
            String c1 = this.deaths;
            Integer n1 = Integer.valueOf(c1);;
            String c2 = o.getDeaths();
            Integer n2 = Integer.valueOf(c2);

            return n1.compareTo(n2);
        }
        
        return result;
    }

    @Override
    public String toString() {
        return state + " , Homicides: " + deaths;
    }
}
