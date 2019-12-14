package com.ashwin.summarization_state_to_avgage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

    
public class App 
{
    public static void main( String[] args )
    {
    	Configuration conf = new Configuration();
    	try {
			Job job = Job.getInstance(conf, "nysestock");
			job.setJarByClass(App.class);
			
			//Set Mappers and Reducer classes
			job.setMapperClass(StockMapper.class);
			job.setReducerClass(StockReducer.class);
                                job.setCombinerClass(StockCombiner.class);
                                
                        TextInputFormat.addInputPath(job, new Path(args[0]));
			//Set InputFormat and OutputFormat class
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			//Set the output key and value types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(StockTuple.class);
                        job.setMapOutputKeyClass(Text.class);
                        job.setMapOutputValueClass(StockTuple.class);
			job.setOutputFormatClass(TextOutputFormat.class);
                        
			//Set the input and output paths
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
                        
			//Set the number of reducers
			job.setNumReduceTasks(1);
			
			try {
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
        System.out.println( "Success!" );
    }
}
