package com.ashwin.deadliest_weapons;

import com.ashwin.frequent_weapons.*;
import com.ashwin.violent_city.*;
	import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
	import org.apache.hadoop.io.LongWritable; 
	import org.apache.hadoop.io.Text; 
	import org.apache.hadoop.mapreduce.Job; 
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
	import org.apache.hadoop.util.GenericOptionsParser; 

	public class App { 

		public static void main(String[] args) throws Exception 
		{ 
			Configuration conf = new Configuration(); 

			Job job = Job.getInstance(conf, "top 10"); 
			job.setJarByClass(App.class); 

			job.setMapperClass(DeadliestWeaponMapper.class); 
			job.setReducerClass(DeadliestWeaponReducer.class); 

			job.setMapOutputKeyClass(Text.class); 
			job.setMapOutputValueClass(LongWritable.class); 

			job.setOutputKeyClass(LongWritable.class); 
			job.setOutputValueClass(Text.class); 

			FileInputFormat.addInputPath(job, new Path(args[0])); 
			FileOutputFormat.setOutputPath(job, new Path(args[1])); 

			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(args[1]),true);

			
			System.exit(job.waitForCompletion(true) ? 0 : 1); 
		} 
	} 

