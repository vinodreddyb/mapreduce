package com.vinod.hadoop.mapreduce.example.datatypes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BigramJob implements Tool {
	private Configuration conf;
	@Override
	public Configuration getConf() {
		
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		Job bigramJob = new Job(getConf());
		bigramJob.setJobName("Bigram Count");
		bigramJob.setJarByClass(this.getClass());
		bigramJob.setMapperClass(BigramMapper.class);
		bigramJob.setReducerClass(BigramReducer.class);

		bigramJob.setMapOutputKeyClass(BiGram.class);
		bigramJob.setMapOutputValueClass(LongWritable.class);
		bigramJob.setOutputKeyClass(BiGram.class);
		bigramJob.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(bigramJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(bigramJob, new Path(args[1]));
		return bigramJob.waitForCompletion(true) == true ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(),new  BigramJob(), args);
	}

}
