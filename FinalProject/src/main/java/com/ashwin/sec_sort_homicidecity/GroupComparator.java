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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator{

	protected GroupComparator() {
		super(CompositeKey.class, true);
	}
@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		CompositeKey ckw1 = (CompositeKey)a;
		CompositeKey ckw2 = (CompositeKey)b;
		
		return ckw1.getState().compareTo(ckw2.getState());
	}
}
	
