package com.ashwin.homicide_by_weapon;

import com.ashwin.homicide_by_city.*;
import com.ashwin.homicide_by_year.*;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ByWeaponMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text word = new Text();
    IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");
        if (tokens[0].equals("Record ID")) {
            return;
        }

        String Weapon = tokens[20];
        word.set(Weapon);
        context.write(word, one);
    }
}
