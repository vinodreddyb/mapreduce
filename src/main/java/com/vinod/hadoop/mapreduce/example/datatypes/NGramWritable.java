package com.vinod.hadoop.mapreduce.example.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public class NGramWritable implements WritableComparable<NGramWritable>{

	private String[] values;
	
	public void set(List<String> ngrams) {
		values = new String[ngrams.size()];
		for(int i =0; i < ngrams.size(); i++) {
			values[i] = ngrams.get(i);
		}
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		 values = new String[in.readInt()];          // construct values
		    for (int i = 0; i < values.length; i++) {
		                           // read a value
		      values[i] = in.readUTF();                          // store it in values
		    }
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(values.length);                 // write values
	    for (int i = 0; i < values.length; i++) {
	      out.writeUTF(values[i]);
	    }
	}

	@Override
	public int compareTo(NGramWritable other) {
		String[] wl = this.getValues();
		String[] w2 = other.getValues();
	    int cmp = 0;
		int len = Math.min(wl.length, w2.length);
		for(int i=0; i<len; i++) {
			if(cmp == 0) {
				cmp = wl[i].compareTo(w2[i]);
			} else {
				break;
			}
							
		}
		return cmp;
	}
	
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(values);
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NGramWritable other = (NGramWritable) obj;
		if (!Arrays.equals(values, other.values))
			return false;
		return true;
	}
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for(String s : values) {
			builder.append(s);
		}
		return builder.toString();
	}
	

	public String[] getValues() {
		return values;
	}

	public void setValues(String[] values) {
		this.values = values;
	}
	
}
