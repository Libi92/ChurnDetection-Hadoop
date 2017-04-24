package com.chinnu.churndetection.mewfinder;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MewReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {

	public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		for (DoubleWritable value : values) {
			context.write(key, value);
		}

	}

}
