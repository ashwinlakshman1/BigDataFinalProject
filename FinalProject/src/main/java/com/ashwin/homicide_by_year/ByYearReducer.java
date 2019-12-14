package com.ashwin.homicide_by_year;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ByYearReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        IntWritable count = new IntWritable(sum);
        context.write(key, count);
    }

}
