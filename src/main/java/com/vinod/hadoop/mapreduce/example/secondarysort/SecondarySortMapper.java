package com.vinod.hadoop.mapreduce.example.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable, Text, StockKey, DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		String symbol = tokens[0].trim();
		Long timeStamp = Long.parseLong(tokens[1].trim());
		Double stockValue = Double.parseDouble(tokens[2].trim());
		
		StockKey skey = new StockKey(new Text(symbol), timeStamp);
		context.write(skey, new DoubleWritable(stockValue));
		
		
	}
}
