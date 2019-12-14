/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ashwin.sec_sort_homicidecity;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author ashwi
 */
public class SecondarySortReducer extends Reducer<CompositeKey, Text, CompositeKey, Text> {

    @Override
    protected void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //To change body of generated methods, choose Tools | Templates.
        int sum = 0;
        for (Text v : values) {
            
            context.write(key, v);
        }
    }
}
