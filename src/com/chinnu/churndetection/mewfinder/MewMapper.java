package com.chinnu.churndetection.mewfinder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

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

public class MewMapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
	private static String INPUT = ChurnDriver.INPUT_DIR + "data.csv";
    
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] concepts = line.split(",");
		int item = Integer.parseInt(concepts[Constants.ITEM_INDEX]);
		int dayCalls = Integer.parseInt(concepts[Constants.DAY_CALL_INDEX]);
		int eveCalls = Integer.parseInt(concepts[Constants.EVE_CALL_INDEX]);
		int nightCalls = Integer.parseInt(concepts[Constants.NIGHT_CALL_INDEX]);
		int intrCalls = Integer.parseInt(concepts[Constants.INTR_CALL_INDEX]);
		
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path inputPath = new Path(INPUT);
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(inputPath)));
		String inLine;
		
		int etahDay = 0;
		int etahEve = 0;
		int etahNight = 0;
		int etahIntr = 0;
		
		int mDay = 0;
		int mEve = 0;
		int mNight = 0;
		int mIntr = 0;
		
		while ((inLine = bufferedReader.readLine()) != null) {
			String[] split = inLine.split(",");
			
			int xDayCalls = Integer.parseInt(split[Constants.DAY_CALL_INDEX]);
			int xEveCalls = Integer.parseInt(split[Constants.EVE_CALL_INDEX]);
			int xNightCalls = Integer.parseInt(split[Constants.NIGHT_CALL_INDEX]);
			int xIntrCalls = Integer.parseInt(split[Constants.INTR_CALL_INDEX]);
			
			if(xDayCalls < dayCalls){
				etahDay += xDayCalls;
			}
			
			if(xEveCalls < eveCalls){
				etahEve += xEveCalls;
			}
			
			if(xNightCalls < nightCalls){
				etahNight += xNightCalls;
			}
			
			if(xIntrCalls < intrCalls){
				etahIntr += xIntrCalls;
			}
			
			mDay += xDayCalls;
			mEve += xEveCalls;
			mNight += xNightCalls;
			mIntr += xIntrCalls;
		}
		
		int etahEveNight = etahEve + etahNight;
		int mEveNight = mEve + mNight;
		
		int maxEtah = 0, maxM = 0;
		if(etahDay > maxEtah){
			maxEtah = etahDay;
			maxM = mDay;
		}
		if(etahEveNight > maxEtah){
			maxEtah = etahEveNight;
			maxM = mEveNight;
		}
		if(etahIntr > maxEtah){
			maxEtah = etahIntr;
			maxM = mIntr;
		}
		
		double mewX = maxEtah / (double)maxM;
		DoubleWritable outValue = new DoubleWritable(mewX);
		
		LongWritable key = new LongWritable(item);
		context.write(key, outValue);
		
	}

}
