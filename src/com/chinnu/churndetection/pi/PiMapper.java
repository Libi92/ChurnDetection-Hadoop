package com.chinnu.churndetection.pi;

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

public class PiMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	private static String INPUT = ChurnDriver.MEW_OUTPUT_DIR + "part-r-00000";
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String value = ivalue.toString();
		String[] split = value.split("\t");
		IntWritable key = new IntWritable(Integer.parseInt(split[0]));
		double mew = Double.parseDouble(split[1]);
				
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path inputPath = new Path(INPUT);
		BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(inputPath)));
		
		String line;
		while ((line = br.readLine()) != null) {
			split = line.split("\t");
			double xMew = Double.parseDouble(split[1]);
			double dif = Math.abs((mew - xMew));
			
			context.write(key, new DoubleWritable(dif));
		}
		
	}

}
