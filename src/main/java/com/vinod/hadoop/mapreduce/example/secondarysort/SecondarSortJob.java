package com.vinod.hadoop.mapreduce.example.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Imagine we have stock data that looks like the following. Each line represents the value of a stock at a particular time. 
 * Each value in a line is delimited by a comma. 
 * The first value is the stock symbol (i.e. GOOG), the second value is the timestamp (i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT), 
 * and the third value is the stock’s price. 
 * The data below is a toy data set. As you can see, there are 3 stock symbols: a, b, and c. 
 * The timestamps are also simple: 1, 2, 3, 4. The values are fake as well: 1.0, 2.0, 3.0, and 4.0.
a, 1, 1.0
b, 1, 1.0
c, 1, 1.0
a, 2, 2.0
b, 2, 2.0
c, 2, 2.0
a, 3, 3.0
b, 3, 3.0
c, 3, 3.0
a, 4, 4.0
b, 4, 4.0
c, 4, 4.0
Let’s say we want for each stock symbol (the reducer key input, or alternatively, the mapper key output), 
to order the values descendingly by timestamp when they come into the reducer. 
How do we sort the timestamp descendingly? 
This problem is known as secondary sorting. Hadoop’s M/R platform sorts the keys, but not the values. 

 * @author vinod
 *
 */
public class SecondarSortJob extends Configured implements Tool {

	/**
	 * Main method. You should specify -Dmapred.input.dir and -Dmapred.output.dir.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new SecondarSortJob(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "secondary sort");
		
		job.setJarByClass(SecondarSortJob.class);
		job.setPartitionerClass(StockKeyPartitioner.class);
		job.setGroupingComparatorClass(StockKeyGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		
		job.setMapOutputKeyClass(StockKey.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(SecondarySortMapper.class);
		job.setReducerClass(SecondarySortReducer.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) == true ? 0 : -1;
	}

}
