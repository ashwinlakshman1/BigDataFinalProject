package com.ashwin.top_deadliest;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopDeadliestMapper extends Mapper<Object, Text, Text, Text> {

    private TreeMap<String, String> tmap;

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
        tmap = new TreeMap<String, String>();
    }

    @Override
    public void map(Object key, Text value,
            Context context) throws IOException,
            InterruptedException {


        String[] tokens = value.toString().split("\t");

        String ratio = tokens[0];
        String details = tokens[1];

        tmap.put(ratio, details);

        if (tmap.size() > 10) {
            tmap.remove(tmap.firstKey());
        }
    }

    //Called once at the end of the task
    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Map.Entry<String, String> entry : tmap.entrySet()) {

            String rat = entry.getKey();
            String w_details = entry.getValue();

            context.write(new Text(rat), new Text(w_details));
        }
    }
}
