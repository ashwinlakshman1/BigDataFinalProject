package com.ashwin.summarization_state_to_avgage;

import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockReducer extends Reducer<Text,StockTuple,Text,StockTuple>{
    private StockTuple stTuple = new StockTuple();

	protected void reduce(Text key, Iterable<StockTuple> values,Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

            long count=0;
            long total_age=0;
            for(StockTuple st:values)
            {
                count+=st.getCount();
                total_age=st.getAge();
            }

            stTuple.setAge(total_age/count);
            stTuple.setCount(count);
            context.write(key, stTuple);
    }

}
