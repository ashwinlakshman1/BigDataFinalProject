/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ashwin.sec_sort_homicidecity;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;

/**
 *
 * @author ashwi
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, Text>{
    IntWritable one = new IntWritable(1);
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         //To change body of generated methods, choose Tools | Templates.
         String line = value.toString();
        String[] tokens = line.split(",");


                   String city = tokens[0];
                   String []details= tokens[1].split("\t");
                   String state= details[0];
                   String deaths=details[1];

             CompositeKey coKey = new CompositeKey(state, deaths);

             context.write(coKey, new Text(city));

         }
         
    }
    
    

