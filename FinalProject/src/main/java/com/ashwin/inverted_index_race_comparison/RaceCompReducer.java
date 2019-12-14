package com.ashwin.inverted_index_race_comparison;

import com.ashwin.homicide_by_weapon.*;
import com.ashwin.homicide_by_city.*;
import com.ashwin.homicide_by_year.*;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RaceCompReducer extends Reducer<Text, Text, Text, Text> {
HashSet<String> hs = new HashSet<String>();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        hs.clear();
        int sum = 0;
//        for (IntWritable val : values) {
//            sum += val.get();
//        }
     StringBuffer sb = new StringBuffer("");
        for(Text v: values){
            hs.add(v.toString());
            sum+=1;

        }

        for(String v: hs){
            sb.append(v);
            sb.append(" ");
        }

        context.write(key, new Text(sb.toString()+ " events:"+ Integer.toString(sum)));

    }

}
