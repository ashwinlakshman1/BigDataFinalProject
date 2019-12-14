package com.ashwin.inner_join_weapon_deadliness;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DeadliestMapper extends Mapper<LongWritable, Text, Text, Text> {

    //    hadoop datatype
    Text word = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens= line.split("\t");
        String newKey = tokens[0];
        word.set(newKey);
        String outValue= "A"+tokens[1];
        context.write(word,new Text(outValue));
    }

}
