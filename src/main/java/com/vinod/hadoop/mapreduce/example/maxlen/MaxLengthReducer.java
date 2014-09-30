package com.vinod.hadoop.mapreduce.example.maxlen;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxLengthReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

	
	String maxKey;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
        maxKey = "";
	}

	@Override
	protected void reduce(Text key, Iterable<NullWritable> iterate,Context context)
			throws IOException, InterruptedException {
		if(key.toString().length() > maxKey.length()) {
			maxKey = key.toString();
		}
		System.out.println(maxKey);
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		context.write(new Text(maxKey), NullWritable.get());
	}

}
