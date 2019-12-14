package com.ashwin.inner_join_weapon_deadliness;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeadlinessReducer extends Reducer<Text, Text, Text, Text> {

    private static final Text EMPTY_TEXT = new Text(" ");
    private Text tmp = new Text();
    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();
    private String joinType = null;

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
        joinType = "inner";
    }

    @Override
    public void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {

        listA.clear();
        listB.clear();
        Iterator<Text> itr = values.iterator();
        while (itr.hasNext()) {
            tmp = itr.next();

            if (tmp.charAt(0) == 'A') {
                listA.add(new Text(tmp.toString().substring(1)));
            } else if (tmp.charAt(0) == 'B') {
                listB.add(new Text(tmp.toString().substring(1)));
            }
        }
        //JOIN LOGIC
        for (Text textA : listA) {
            for (Text textB : listB) {
                float dd = Float.parseFloat(textA.toString()) / Float.parseFloat(textB.toString());
                String x = "Weapon : " + key.toString() + "  Deaths :  " + textA.toString() + "  Incidents :  " + textB.toString();
                Text t1 = new Text(x);
                String dds = dd + " ";
                Text ddscore = new Text(dds);
                context.write(ddscore, t1);
            }
        }
    }

}
