package com.ashwin.inverted_index_race_comparison;

import com.ashwin.homicide_by_weapon.*;
import com.ashwin.homicide_by_city.*;
import com.ashwin.homicide_by_year.*;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RaceCompMapper extends Mapper<LongWritable, Text, Text, Text> {


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String perp_race;
        String vic_race;
        String[] tokens = line.split(",");
        if (tokens[0].equals("Record ID")) {
            return;
        }

     if(tokens[18].equals("Hispanic")){
         perp_race=tokens[18];
     }
     else perp_race=tokens[17];
     
     if(tokens[14].equals("Hispanic")){
         vic_race=tokens[14];
     }
     else vic_race=tokens[13];

        context.write(new Text(perp_race+":"), new Text(vic_race));

    }
}
