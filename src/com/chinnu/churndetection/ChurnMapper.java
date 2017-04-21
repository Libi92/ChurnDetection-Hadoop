package com.chinnu.churndetection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChurnMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static String BASEURL = "/user/libin/ChurnDetection/";
    private static String INPUT = BASEURL + "raw/data.csv";
    
    private static int DAY_CALL_INDEX = 9;
    private static int EVE_CALL_INDEX = 12;
    private static int NIGHT_CALL_INDEX = 15;
    private static int INTR_CALL_INDEX = 18;
    
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] concepts = line.split(",");
		int dayCalls = Integer.parseInt(concepts[DAY_CALL_INDEX]);
		int eveCalls = Integer.parseInt(concepts[EVE_CALL_INDEX]);
		int nightCalls = Integer.parseInt(concepts[NIGHT_CALL_INDEX]);
		int intrCalls = Integer.parseInt(concepts[INTR_CALL_INDEX]);
		
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path inputPath = new Path(INPUT);
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(inputPath)));
		String inLine;
		
		while ((inLine = bufferedReader.readLine()) != null) {
			String[] split = inLine.split(",");
			
			int xDayCalls = Integer.parseInt(split[DAY_CALL_INDEX]);
			int xEveCalls = Integer.parseInt(split[EVE_CALL_INDEX]);
			int xNightCalls = Integer.parseInt(split[NIGHT_CALL_INDEX]);
			int xIntrCalls = Integer.parseInt(split[INTR_CALL_INDEX]);
		}
		
		for (String word : words) {
			Text outputKey = new Text(word.toUpperCase().trim());
			IntWritable outputValue = new IntWritable(1);
			context.write(outputKey, outputValue);
			
			
		}
	}

}
