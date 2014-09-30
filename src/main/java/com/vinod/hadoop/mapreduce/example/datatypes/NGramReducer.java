package com.vinod.hadoop.mapreduce.example.datatypes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramReducer extends Reducer<NGramWritable,LongWritable , NGramWritable, LongWritable> {

	@Override
	protected void reduce(NGramWritable key, Iterable<LongWritable> values,Context context)
			throws IOException, InterruptedException {
		int count=0;
        for(LongWritable value: values)
        {
            count += value.get();
        }
 
       
        context.write(key, new LongWritable(count));
	}
}
