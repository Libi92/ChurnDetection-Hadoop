package com.chinnu.churndetection.pi;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PiReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

	public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double pi = 0d;
		for (DoubleWritable val : values) {
			pi += val.get();
		}
		
		context.write(key, new DoubleWritable(pi));
	}

}
