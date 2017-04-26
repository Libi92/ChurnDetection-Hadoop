package com.chinnu.churndetection.mountaincentroids;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.chinnu.churndetection.ChurnDriver;
import com.chinnu.churndetection.utils.Constants;
import com.chinnu.churndetection.utils.MountainWritable;

public class MountainCentroidMapper
		extends Mapper<LongWritable, Text, IntWritable, MountainWritable> {

	private double T1;
	private String inputText;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, IntWritable, MountainWritable>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		T1 = conf.getDouble(Constants.T1, 0);
		inputText = conf.get(Constants.INPUT_TEXT);
		
		super.setup(context);
	}
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] concepts = line.split(",");
		int item = Integer.parseInt(concepts[Constants.ITEM_INDEX]);
		int dayCalls = Integer.parseInt(concepts[Constants.DAY_CALL_INDEX]);
		int eveCalls = Integer.parseInt(concepts[Constants.EVE_CALL_INDEX]);
		int nightCalls = Integer.parseInt(concepts[Constants.NIGHT_CALL_INDEX]);
		int intrCalls = Integer.parseInt(concepts[Constants.INTR_CALL_INDEX]);
		
		String[] data = inputText.split("\n");
		
		for(int i = 0; i < data.length; i++) {
			String inLine = data[i];
			String[] split = inLine.split(",");
			
			int xDayCalls = Integer.parseInt(split[Constants.DAY_CALL_INDEX]);
			int xEveCalls = Integer.parseInt(split[Constants.EVE_CALL_INDEX]);
			int xNightCalls = Integer.parseInt(split[Constants.NIGHT_CALL_INDEX]);
			int xIntrCalls = Integer.parseInt(split[Constants.INTR_CALL_INDEX]);
			
			double dayDif = Math.pow((dayCalls - xDayCalls), 2);
			double eveDif = Math.pow((eveCalls - xEveCalls), 2);
			double nightDif = Math.pow((nightCalls - xNightCalls), 2);
			double intrDif = Math.pow((intrCalls - xIntrCalls), 2);
			
			double sum = dayDif + eveDif + nightDif + intrDif;
			
			double di = Math.sqrt(sum);
			double denom = Math.pow((T1 / 2), 2);
			double exp = (di / denom);

			IntWritable key = new IntWritable(item);
			MountainWritable val = new MountainWritable();
			val.setExp(exp);
			val.setDistance(di);
			
			context.write(key, val);
		}
	}

}
