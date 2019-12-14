package com.ashwin.summarization_state_to_avgage;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockMapper extends Mapper<Object, Text, Text, StockTuple> {

    private StockTuple stTuple = new StockTuple();
    //private final static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");
        if (tokens[0].equals("Record ID")) {
            return;
        }
        String city = tokens[5];
        String age = tokens[16];
        int intage = 28;
        if (age.length() < 3 && !age.equals(" ")) {
            intage = Integer.parseInt(age);
            if (intage == 0) {
            intage = 28;
        }
        }
        

        stTuple.setCount(1);
        stTuple.setAge(intage);
        context.write(new Text(city), stTuple);

    }
}
