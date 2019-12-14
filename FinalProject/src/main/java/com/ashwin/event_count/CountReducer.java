package com.ashwin.event_count;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {

    @Override
    protected void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        IntWritable count = new IntWritable(sum);
        context.write(key, count);
    }

}
