package com.vinod.hadoop.mapreduce.example.datatypes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NGramJob implements Tool{

	 public static final String GRAM_LENGTH = "number_of_grams";
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
		conf.setInt(GRAM_LENGTH, Integer.parseInt(args[2]));

		Job ngramJob = new Job(getConf());
		ngramJob.setJobName("Ngram Count");
		ngramJob.setJarByClass(this.getClass());
		ngramJob.setMapperClass(NgramMapper.class);
		ngramJob.setReducerClass(NGramReducer.class);

		ngramJob.setMapOutputKeyClass(NGramWritable.class);
		ngramJob.setMapOutputValueClass(LongWritable.class);
		ngramJob.setOutputKeyClass(NGramWritable.class);
		ngramJob.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(ngramJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(ngramJob, new Path(args[1]));
		return ngramJob.waitForCompletion(true) == true ? 0 : -1;
	}
	
	
	
	public static void main(String[] args) throws Exception {
        if(args.length != 3) {
            
            System.exit(1);
        }
       ToolRunner.run(new Configuration(), new NGramJob(), args);
   }

}
