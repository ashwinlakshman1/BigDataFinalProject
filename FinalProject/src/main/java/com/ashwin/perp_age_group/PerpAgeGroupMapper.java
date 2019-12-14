package com.ashwin.perp_age_group;

import com.ashwin.homicide_by_weapon.*;
import com.ashwin.homicide_by_city.*;
import com.ashwin.homicide_by_year.*;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PerpAgeGroupMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text word = new Text();
    IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");
        if (tokens[0].equals("Record ID")) {
            return;
        }

//        word.set(tokens[16]);
        if (tokens[16].length() < 4 && !tokens[16].equals(" ")) {
            int age = 0;
            
            age = Integer.parseInt(tokens[16]);

            String group = "";

            if (age > 0 && age <= 12) {
                group = "Children";
            } else if (age > 12 && age <= 18) {
                group = "Teenager";
            } else if (age > 18 && age < 30) {
                group = "Young Adult";
            } else if (age >= 30 && age < 60) {
                group = "Mature Adult";
            } else if (age >= 60) {
                group = "Senior";
            }
            else group="Unknown";

            word.set(group);
        } else {
            word.set("Unknown");
        }

        context.write(word, one);

    }
}
