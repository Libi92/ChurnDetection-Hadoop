package com.chinnu.churndetection.mountainfunction;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MountainReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

	public void reduce(IntWritable _key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double MlXi = 0d;
		for (DoubleWritable val : values) {
			MlXi += val.get();
		}
		
		context.write(_key, new DoubleWritable(MlXi));
	}

}
