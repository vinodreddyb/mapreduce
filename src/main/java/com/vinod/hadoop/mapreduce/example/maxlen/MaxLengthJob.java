package com.vinod.hadoop.mapreduce.example.maxlen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxLengthJob implements Tool{
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
		Job maxWord = new Job(getConf());
		maxWord.setJobName("Kelly Word Count");
		maxWord.setJarByClass(this.getClass());
		maxWord.setMapperClass(MaxLengthMapper.class);
		maxWord.setReducerClass(MaxLengthReducer.class);
		maxWord.setMapOutputKeyClass(Text.class);
		maxWord.setMapOutputValueClass(NullWritable.class);
		maxWord.setOutputKeyClass(Text.class);
		maxWord.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(maxWord, new Path(args[0]));
		FileOutputFormat.setOutputPath(maxWord, new Path(args[1]));
		return maxWord.waitForCompletion(true) == true ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(),new  MaxLengthJob(), args);
	}

}
