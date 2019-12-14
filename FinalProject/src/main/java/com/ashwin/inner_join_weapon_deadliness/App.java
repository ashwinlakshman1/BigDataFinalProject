package com.ashwin.inner_join_weapon_deadliness;

import com.ashwin.deadliest_weapons.*;
import com.ashwin.frequent_weapons.*;
import com.ashwin.violent_city.*;
	import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
	import org.apache.hadoop.io.LongWritable; 
	import org.apache.hadoop.io.Text; 
	import org.apache.hadoop.mapreduce.Job; 
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
	import org.apache.hadoop.util.GenericOptionsParser; 

	public class App { 

		public static void main(String[] args) throws Exception 
		{ 
			Configuration conf = new Configuration(); 
			Job job = new Job(conf, "Inner Join-Weapon Deadliness"); 
			job.setJarByClass(App.class); 
			job.setMapperClass(FrequentMapper.class); 
			job.setReducerClass(DeadlinessReducer.class); 
			job.setMapOutputKeyClass(Text.class); 
			job.setMapOutputValueClass(Text.class); 
			job.setOutputKeyClass(Text.class); 
			job.setOutputValueClass(Text.class); 




        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, DeadliestMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, FrequentMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

                        
		} 
	} 

