package com.chinnu.churndetection.mountaincentroids;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.chinnu.churndetection.utils.Constants;
import com.chinnu.churndetection.utils.MountainWritable;

public class MountainCentroidReducer extends Reducer<IntWritable, MountainWritable, IntWritable, DoubleWritable> {

	private double M1, T1;
	@Override
	protected void setup(Reducer<IntWritable, MountainWritable, IntWritable, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		M1 = conf.getDouble(Constants.M1, 0);
		T1 = conf.getDouble(Constants.T1, 0);
		super.setup(context);
	}
	
	public void reduce(IntWritable _key, Iterable<MountainWritable> values, Context context) throws IOException, InterruptedException {
		double MlXi = 0d;
		double maxD = Double.MIN_VALUE;
		for (MountainWritable val : values) {
			MlXi += val.getExp();
			if(val.getDistance() > maxD){
				maxD = val.getDistance();
			}
		}
		
		double T2 = 1.5 * T1;
		double denom = Math.pow((T2 / 2), 2);
		double Mli = MlXi - M1 * Math.exp(-(maxD / denom));
		
		context.write(_key, new DoubleWritable(Mli));
	}

}
