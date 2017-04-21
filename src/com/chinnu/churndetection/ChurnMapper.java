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
	static String BASEURL = "/user/libin/ChurnDetection/";
    static String INPUT = BASEURL + "raw/input.csv";
    
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] words = line.split(",");
		for (String word : words) {
			Text outputKey = new Text(word.toUpperCase().trim());
			IntWritable outputValue = new IntWritable(1);
			context.write(outputKey, outputValue);
			
			Configuration conf = new Configuration();
			FileSystem fileSystem = FileSystem.get(conf);
			Path inputPath = new Path(INPUT);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(inputPath)));
			String inLine;
			
			while ((inLine = bufferedReader.readLine()) != null) {
				
			}
		}
	}

}
