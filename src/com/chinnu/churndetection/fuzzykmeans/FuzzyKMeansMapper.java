/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.chinnu.churndetection.fuzzykmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import com.chinnu.churndetection.utils.Constants;
import com.chinnu.churndetection.utils.DistanceComparator;
import com.chinnu.churndetection.utils.MRLogger;
import com.chinnu.churndetection.utils.Vector;


/**
 *
 * @author libin
 */
public class FuzzyKMeansMapper extends Mapper<LongWritable, Text, IntWritable, Vector> {
    
    String CENTERS;
    int STARTINDEX;
    int ENDINDEX;
    int CLASSINDEX;
    int DATALENGTH;
    double m = 1.2d;

    @Override
    protected void setup(Mapper<LongWritable, Text, IntWritable, Vector>.Context context)
    		throws IOException, InterruptedException {
    	
    	Configuration conf = context.getConfiguration();
    	
    	CENTERS = conf.get(Constants.CENTER_TEXT);
        STARTINDEX = conf.getInt(Constants.STARTINDEX, 0);
        ENDINDEX = conf.getInt(Constants.ENDINDEX, 0);
        CLASSINDEX = conf.getInt(Constants.CLASSINDEX, 0);
        DATALENGTH = ENDINDEX - STARTINDEX;
    	super.setup(context);
    }
    
    
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Vector>.Context context)
    		throws IOException, InterruptedException {
    	   
        HashMap<Integer, double[]> centers = new HashMap<>();
        int idx = 0;
        String[] lineSplit = CENTERS.split("\n");
        for(int j = 0; j < lineSplit.length; j++) {
        	String line = lineSplit[j];
            double[] center = new double[DATALENGTH];
            String[] split = line.split(",");
            for (int i = 0; i < DATALENGTH; i++) {
                center[i] = Double.parseDouble(split[i]);
            }
            centers.put(idx++, center);
            
        }

        
        String line = value.toString();
        String[] split = line.split(",");
        double[] data = new double[DATALENGTH];

        for (int i = STARTINDEX; i < ENDINDEX; i++) {
            data[i - STARTINDEX] = Double.parseDouble(split[i]);
        }
        
        
        double etahSum = 0d;
        for(Integer cKey : centers.keySet()){
        	double[] cenetr = centers.get(cKey);
        	double dist = DistanceComparator.findDistance(cenetr, data);
        	double inv_dist = 1 / dist;
        	double pow = 1 / (m - 1);
        	double etah = Math.pow(inv_dist, pow);
        	etahSum += etah;
        }

        String className = split[CLASSINDEX];

        Vector vector = new Vector();
        vector.setData(data);
        vector.setClassName(className);

        vector.setIndex(Integer.parseInt(split[0]));

        int nearCenter = DistanceComparator.findMinimumDistance(data, centers);
        
        double[] cenetr = centers.get(nearCenter);
    	double dist = DistanceComparator.findDistance(cenetr, data);
    	double inv_dist = 1 / dist;
    	double pow = 1 / (m - 1);
    	double etah = Math.pow(inv_dist, pow);
    	
    	double mew = etah / etahSum;
        vector.setMew(mew);
        MRLogger.Log("Mew : " + mew);
    	
        IntWritable k = new IntWritable(nearCenter);
        context.write(k, vector);
    }

}
