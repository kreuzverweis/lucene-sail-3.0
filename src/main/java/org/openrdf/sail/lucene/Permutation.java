/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;


/**
 *	This class counts through all permutations of the set cardinality.
 * Each added digit has a distinct max value.
 * @author Enrico Minack
 */
public class Permutation {
	private class Digit {
		private final int max;
		private int val = 1;
		
		private Digit(int max) {
			this.max = max;
		}
		
		private boolean next() {
			if(this.val >= this.max) {
				this.val = 1;
				return true;
			}

			this.val++;
			return false;
		}
		
		private int val() {
			if(this.val > this.max)
				return this.max;
			return this.val;
		}
	}
	
	private List<Digit> digits = new ArrayList<Digit>();
	private boolean invalid = false;

	public void addDigit(int max) {
		this.digits.add(new Digit(max));
	}
	
	public Vector<Integer> val() {
		Vector<Integer> val = new Vector<Integer>();
		for(Digit dim : this.digits) {
			val.add(new Integer(dim.val()));
		}
		return val;
	}
	
	public boolean next() {
		if(this.digits.size() == 0)
			return true;
		
		return next(this.digits.iterator());
	}
	
	private boolean next(Iterator<Digit> it) {
		if(it.hasNext()) {
			Digit dim = it.next();
			if(next(it)) {
				if(dim.next())
					return true;
			}
			return false;
		} else {
			return true;
		}
	}
	
	public void invalidate() {
		this.invalid = true;
	}
	
	public boolean isInvalid() {
		return this.invalid;
	}
	
	public String toString() {
		StringBuilder string = new StringBuilder();
		for(Digit digit : this.digits) {
			string.append(digit.val()); string.append(" ");
		}
		return string.toString();
	}

}
