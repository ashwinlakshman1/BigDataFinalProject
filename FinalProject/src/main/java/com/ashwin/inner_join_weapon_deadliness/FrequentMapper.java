package com.ashwin.inner_join_weapon_deadliness;

import java.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequentMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text word = new Text();
    IntWritable one = new IntWritable(1);
    
    @Override
    public void map(LongWritable key, Text value,
            Context context) throws IOException,
            InterruptedException {


                String line = value.toString();
        String[] tokens= line.split("\t");

        String newKey = tokens[0];
        word.set(newKey);
        String outValue= "B"+tokens[1];
        context.write(word,new Text(outValue));
        }
    }


