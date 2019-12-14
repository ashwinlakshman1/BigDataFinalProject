/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ashwin.summarization_state_to_avgage;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ashwi
 */
public class StockCombiner extends Reducer<Text, StockTuple, Text, StockTuple>{
    
    StockTuple stTuple= new StockTuple();
    
    @Override
    protected void reduce(Text key, Iterable<StockTuple> values, Context context)throws IOException,InterruptedException
    {
        long count=0;
        long sum_price=0;
        for(StockTuple st:values){
            count+=st.getCount();
            sum_price+=st.getAge();
            
        }
        stTuple.setCount(count);
        stTuple.setAge(sum_price);
        context.write(key, stTuple);
    }
}
