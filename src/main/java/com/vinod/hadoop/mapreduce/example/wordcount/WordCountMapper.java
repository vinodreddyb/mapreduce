package com.vinod.hadoop.mapreduce.example.wordcount;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {

	private Text temp = new Text();
	private final static LongWritable one = new LongWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String string = value.toString();
		StringTokenizer strTock = new StringTokenizer(string, " ");
		while (strTock.hasMoreTokens()) {
			temp.set(strTock.nextToken());
			context.write(temp, one);
		}
	};
}
