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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SecondarySortComparator extends WritableComparator {
    
       
	protected SecondarySortComparator() {
		super(CompositeKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
            
         

		CompositeKey ck1 = (CompositeKey) a;
		CompositeKey ck2 = (CompositeKey) b;

		int result = ck1.getState().compareTo(ck2.getState());

		if (result == 0) {
			String c1 = ck1.getDeaths();
			Integer n1 = Integer.valueOf(c1);
			String c2 = ck2.getDeaths();
			Integer n2 = Integer.valueOf(c2);
			result = -1*n1.compareTo(n2);
			
		}

		return result;
	}

	
	
}
