package com.ashwin.victim_by_weapon;

import com.ashwin.homicide_by_weapon.*;
import com.ashwin.homicide_by_city.*;
import com.ashwin.homicide_by_year.*;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ByWeaponMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text word = new Text();


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");
        if (tokens[0].equals("Record ID")) {
            return;
        }
        if(tokens[21].length()<4){
        IntWritable victim = new IntWritable(Integer.parseInt(tokens[21])+1);

        String Weapon = tokens[20];
        word.set(Weapon);
        context.write(word, victim);
        }
    }
}
