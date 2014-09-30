package com.vinod.hadoop.mapreduce.example.wordcount;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, LongWritable>{

	@Override
	public int getPartition(Text key, LongWritable value, int noOfReducers) {
		String tempString = key.toString();
		int i = tempString.toLowerCase().charAt(0)-'a';
		if(i<26 && i>=0) {
			return i;
		} else {
			return 0;
		}
		
	}

}
