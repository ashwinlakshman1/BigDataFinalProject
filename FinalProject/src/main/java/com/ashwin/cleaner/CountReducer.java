package com.ashwin.cleaner;

import com.ashwin.event_count.*;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        int sum = 0;
        for (Text val : values) {
        context.write(key, val);
        }


    }

}
