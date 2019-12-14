package com.ashwin.top_deadliest;

import com.ashwin.frequent_weapons.*;
import com.ashwin.violent_city.*;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopDeadliestReducer extends Reducer<Text, Text, FloatWritable, Text> {

    private TreeMap<String, String> tmap2;

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
        tmap2 = new TreeMap<String, String>();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {

        String ratio = key.toString();
        String details = "";

        for (Text val : values) {
            ratio = val.toString();
        }

        tmap2.put(ratio, details);

        if (tmap2.size() > 10) {
            tmap2.remove(tmap2.firstKey());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {

        for (Map.Entry<String, String> entry : tmap2.entrySet()) {

            String ratiox = entry.getKey();
            String detailsx = entry.getValue();
            context.write(new FloatWritable(Float.parseFloat(ratiox)), new Text(detailsx));
        }
    }
}
