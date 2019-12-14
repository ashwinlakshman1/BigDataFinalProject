package com.ashwin.safest_city;

import com.ashwin.violent_city.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SafestCityMapper extends Mapper<Object, Text, Text, LongWritable> {

    private TreeMap<Long, String> tmap;

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
        tmap = new TreeMap<Long, String>(Collections.reverseOrder());
    }

    @Override
    public void map(Object key, Text value,
            Context context) throws IOException,
            InterruptedException {


        String[] tokens = value.toString().split("\t");

        String city = tokens[0];
        long no_of_homicides = Long.parseLong(tokens[1]);

        tmap.put(no_of_homicides, city);

        if (tmap.size() > 10) {
            tmap.remove(tmap.firstKey());
        }
    }

    //Called once at the end of the task
    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Map.Entry<Long, String> entry : tmap.entrySet()) {

            long count = entry.getKey();
            String city_name = entry.getValue();

            context.write(new Text(city_name), new LongWritable(count));
        }
    }
}
