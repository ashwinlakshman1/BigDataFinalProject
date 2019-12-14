/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ashwin.summarization_state_to_avgage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author ashwi
 */
public class StockTuple implements Writable
{

    public long getAge() {
        return Age;
    }

    public void setAge(long Age) {
        this.Age = Age;
    }
      

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
    private long Age;
    private long count;
    
    @Override
    public void readFields(DataInput in) throws IOException {
        Age = in.readLong();
        count = in.readLong();
    }
   @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(Age);
        out.writeLong(count);

    }



    @Override
    public String toString() {
        return  " average Age: "+Age +" Count: "+count;
    }
}
