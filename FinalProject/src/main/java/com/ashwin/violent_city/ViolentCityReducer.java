package com.ashwin.violent_city;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ViolentCityReducer extends Reducer<Text, LongWritable, LongWritable, Text> {

    private TreeMap<Long, String> tmap2;

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
        tmap2 = new TreeMap<Long, String>();
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values,
            Context context) throws IOException, InterruptedException {

        String name = key.toString();
        long count = 0;

        for (LongWritable val : values) {
            count = val.get();
        }

        tmap2.put(count, name);

        if (tmap2.size() > 10) {
            tmap2.remove(tmap2.firstKey());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {

        for (Map.Entry<Long, String> entry : tmap2.entrySet()) {

            long count = entry.getKey();
            String city_name = entry.getValue();
            context.write(new LongWritable(count), new Text(city_name));
        }
    }
}
