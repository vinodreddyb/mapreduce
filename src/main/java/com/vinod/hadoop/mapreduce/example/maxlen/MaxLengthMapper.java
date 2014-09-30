package com.vinod.hadoop.mapreduce.example.maxlen;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxLengthMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {

	String maxWord ;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		maxWord = "";
	}
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		for(String word: words) {
			if(word.length() > maxWord.length()) {
				maxWord = word;
			}
		}
		
		
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		context.write(new Text(maxWord),NullWritable.get());
	}
}
