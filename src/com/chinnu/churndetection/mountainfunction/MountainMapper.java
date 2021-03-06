package com.chinnu.churndetection.mountainfunction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.chinnu.churndetection.utils.Constants;

public class MountainMapper
		extends Mapper<LongWritable, Text, org.apache.hadoop.io.IntWritable, org.apache.hadoop.io.DoubleWritable> {

	private double T1;
	private int xItem;
	private int xDayCalls;
	private int xEveCalls;
	private int xNightCalls;
	private int xIntrCalls;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, IntWritable, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		String X1F = conf.get(Constants.X1F);
		T1 = conf.getDouble(Constants.T1, 0);

		String[] split = X1F.split(",");

		xItem = Integer.parseInt(split[Constants.ITEM_INDEX]);
		xDayCalls = Integer.parseInt(split[Constants.DAY_CALL_INDEX]);
		xEveCalls = Integer.parseInt(split[Constants.EVE_CALL_INDEX]);
		xNightCalls = Integer.parseInt(split[Constants.NIGHT_CALL_INDEX]);
		xIntrCalls = Integer.parseInt(split[Constants.INTR_CALL_INDEX]);
		
		super.setup(context);
	}
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String value = ivalue.toString();
		String[] concepts = value.split(",");
		int dayCalls = Integer.parseInt(concepts[Constants.DAY_CALL_INDEX]);
		int eveCalls = Integer.parseInt(concepts[Constants.EVE_CALL_INDEX]);
		int nightCalls = Integer.parseInt(concepts[Constants.NIGHT_CALL_INDEX]);
		int intrCalls = Integer.parseInt(concepts[Constants.INTR_CALL_INDEX]);

		double dayDif = Math.pow((dayCalls - xDayCalls), 2);
		double eveDif = Math.pow((eveCalls - xEveCalls), 2);
		double nightDif = Math.pow((nightCalls - xNightCalls), 2);
		double intrDif = Math.pow((intrCalls - xIntrCalls), 2);
		
		double sum = dayDif + eveDif + nightDif + intrDif;
		
		double di = Math.sqrt(sum);
		double denom = Math.pow((T1 / 2), 2);
		double exp = (di / denom);

		IntWritable key = new IntWritable(xItem);
		DoubleWritable val = new DoubleWritable(exp);
		
		context.write(key, val);
	}

}
