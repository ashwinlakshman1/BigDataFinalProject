package com.ashwin.cleaner;

import com.ashwin.event_count.*;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	Configuration conf = new Configuration();
    	try {           //create a new job
			Job job = Job.getInstance(conf, "logcount");
			job.setJarByClass(App.class);
                    			
			//Set Mappers and Reducer classes
			job.setMapperClass(CountMapper.class);
			job.setReducerClass(CountReducer.class);
			
			//Set InputFormat and OutputFormat class OK
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			//Set the output key and value types OK
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
                        job.setMapOutputKeyClass(NullWritable.class);
                        job.setMapOutputValueClass(Text.class);
			
			//Set the input and output paths OK
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			//Set the number of reducers
//			job.setNumReduceTasks(2);
			
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
    	
        System.out.println( "SUCCESS" );
    }
}
