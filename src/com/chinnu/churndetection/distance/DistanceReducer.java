package com.chinnu.churndetection.distance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DistanceReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

	public void reduce(IntWritable _key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		double _d = 0d;
		double n = 0d;
		List<Double> vals = new ArrayList<>();
		for (DoubleWritable val : values) {
			double d = val.get(); 
			_d += d;
			vals.add(d);
			n++;
		}
		
		_d = _d / n;
		
		double T1 = 0d;
		for (double val : vals) {
			T1 += (val - _d);	
		}
		
		T1 = T1 / n;
		
		context.write(_key, new DoubleWritable(T1));
		
	}
}
