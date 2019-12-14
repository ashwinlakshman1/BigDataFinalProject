package com.ashwin.cleaner;

import com.ashwin.event_count.*;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    Text word = new Text();
    IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String city, state, year, solved, vicsex, vicage, vicrace, perpsex, perpage, perprace,newLine;
        String[] tokens = line.split(",");
        if (tokens[0].equals("Record ID")) {
            return;
        }
        state = tokens[5];
        city = tokens[4];
        year = tokens[6];
        solved = tokens[10];
        vicsex = tokens[11];
        vicage = tokens[12];
        vicrace = tokens[13];
        perpsex = tokens[15];
        perpage = tokens[16];
        perprace = tokens[17];
        if(state.equals(" ")||city.equals(" ")||year.equals(" ")||solved.equals(" ")||vicage.equals(" ")||vicrace.equals(" ")||vicsex.equals(" ")||perpage.equals(" ")||perprace.equals(" ")||perpsex.equals(" ")){
            return;
        }
newLine=state+","+city+","+year+","+solved+","+vicsex+","+vicage+","+vicrace+","+perpsex+","+perpage+","+perprace;
        context.write(NullWritable.get(), new Text(newLine));
    }
}
