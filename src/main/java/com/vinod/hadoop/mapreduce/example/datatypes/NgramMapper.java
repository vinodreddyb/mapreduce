package com.vinod.hadoop.mapreduce.example.datatypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class NgramMapper extends Mapper<LongWritable, Text, NGramWritable, LongWritable> {

    private int gram_length;
    private Pattern space_pattern = Pattern.compile("[ ]");
    private NGramWritable ngramWritable = new NGramWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       gram_length = context.getConfiguration().getInt(NGramJob.GRAM_LENGTH, 0);
    }

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        String[] tokens = space_pattern.split(value.toString().replaceAll("[^\\w\\s-]", ""));
        
        for (int i = 0; i < tokens.length; i++) {
                     
            List<String> ngram = new ArrayList<String>();
            if(i + gram_length <= tokens.length) {
               for(int j = i; j < i + gram_length; j++) {
            	   ngram.add(tokens[j] + " ");
               }
               
               ngramWritable.set(ngram);
               context.write(ngramWritable, new LongWritable(1));
            }
        }

    }

}
