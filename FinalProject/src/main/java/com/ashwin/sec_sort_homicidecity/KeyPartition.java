/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ashwin.sec_sort_homicidecity;

/**
 *
 * @author ashwi
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartition extends Partitioner<CompositeKey, Text>{

	@Override
	public int getPartition(CompositeKey key, Text value, int numPartitions) {
		int hash=key.getState().hashCode();
                int part=hash%numPartitions;
		return part;
		
	}

}
