/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.chinnu.churndetection.fuzzykmeans;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
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
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.chinnu.churndetection.utils.Constants;
import com.chinnu.churndetection.utils.DistanceComparator;
import com.chinnu.churndetection.utils.Vector;

/**
 *
 * @author libin
 */
public class FuzzyKMeansMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Vector> {
    
    String CENTERS;
    int STARTINDEX;
    int ENDINDEX;
    int CLASSINDEX;
    int DATALENGTH;

    @Override
    public void configure(JobConf job) {
        CENTERS = job.get(Constants.CENTER);
        STARTINDEX = job.getInt(Constants.STARTINDEX, 0);
        ENDINDEX = job.getInt(Constants.ENDINDEX, 0);
        CLASSINDEX = job.getInt(Constants.CLASSINDEX, 0);
        DATALENGTH = ENDINDEX - STARTINDEX;
    }
    
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Vector> outputCollector, Reporter reporter) throws IOException {
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        
        HashMap<Integer, double[]> centers = new HashMap<>();
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(CENTERS))));
        String line = br.readLine();
        int idx = 0;
        while (line != null) {
            
            File file = new File("/Users/libin/Documents/Projects/kmeansmapreduce/log.txt");
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(line.getBytes());
            fos.flush();
            fos.close();
            
            double[] center = new double[DATALENGTH];
            String[] split = line.split(",");
            for (int i = 0; i < DATALENGTH; i++) {
                center[i] = Double.parseDouble(split[i]);
            }
            centers.put(idx++, center);
            
            line = br.readLine();
        }

        
        line = value.toString();
        String[] split = line.split(",");
        double[] data = new double[DATALENGTH];

        for (int i = STARTINDEX; i < ENDINDEX; i++) {
            data[i - STARTINDEX] = Double.parseDouble(split[i]);
        }

        String className = split[CLASSINDEX];

        Vector vector = new Vector();
        vector.setData(data);
        vector.setClassName(className);

        vector.setIndex(Integer.parseInt(split[0]));

        int nearCenter = DistanceComparator.findMinimumDistance(data, centers);
        
        IntWritable k = new IntWritable(nearCenter);
        outputCollector.collect(k, vector);
    }

}
