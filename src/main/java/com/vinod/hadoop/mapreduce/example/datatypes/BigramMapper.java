package com.vinod.hadoop.mapreduce.example.datatypes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BigramMapper extends Mapper<LongWritable, Text, BiGram, LongWritable> {

	
 
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		Text lastWord = null;
	    BiGram textPair = new BiGram();
	    Text wordText = new Text();
	    LongWritable one = new LongWritable(1);
		 String line = value.toString();
	       /* line = line.replace(",", "");
	        line = line.replace(".", "");*/
		 line.replaceAll("[^\\w\\s-]", "");
	 
		 
	        for(String word: line.split("\\W+"))
	        {
	            if(lastWord == null)
	            {
	                lastWord = new Text(word);
	            }
	            else
	            {
	                wordText.set(word);
	                textPair.set(lastWord, wordText);
	                context.write(textPair, one);
	                lastWord.set(wordText.toString());
	            }
	        }
	}
}
