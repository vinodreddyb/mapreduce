package com.vinod.hadoop.mapreduce.example.datatypes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NGramPartitioner extends Partitioner<NGramWritable, LongWritable> {

	@Override
	public int getPartition(NGramWritable key, LongWritable arg1, int arg2) {
		String tempString = key.toString();
		int i = tempString.toLowerCase().charAt(0)-'a';
		if(i<26 && i>=0) {
			return i;
		} else {
			return 0;
		}
	}

}
