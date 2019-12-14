/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ashwin.sec_sort_homicidecity;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author ashwi
 */
public class App {
    
        public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Job job = Job.getInstance();
        
        job.setJarByClass(App.class);
        
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setSortComparatorClass(SecondarySortComparator.class);
        job.setPartitionerClass(KeyPartition.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outDir);
        
        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(CompositeKey.class);
        job.setOutputValueClass(Text.class);
        
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if(fs.exists(outDir)) {
        	fs.delete(outDir, true);
        }
        
        job.waitForCompletion(true);
    }
}
