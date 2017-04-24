/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.chinnu.churndetection.fuzzykmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.chinnu.churndetection.utils.Constants;
import com.chinnu.churndetection.utils.DistanceComparator;
import com.chinnu.churndetection.utils.Vector;

/**
 *
 * @author libin
 */
public class FuzzyKMeansReducer extends MapReduceBase implements Reducer<IntWritable, Vector, IntWritable, Text> {

    String NEW_CENTER;
    String CURR_CENTER;
    int STARTINDEX;
    int ENDINDEX;
    int DATALENGTH;
    
    static String BASEURL = "/user/hue/KMeansMR/";
    static String CENTER_CONVERGED = BASEURL + "converged.txt";

    @Override
    public void configure(JobConf job) {
        NEW_CENTER = job.get(Constants.NEXTCENTER);
        CURR_CENTER = job.get(Constants.CENTER);
        STARTINDEX = job.getInt(Constants.STARTINDEX, 0);
        ENDINDEX = job.getInt(Constants.ENDINDEX, 0);
        DATALENGTH = ENDINDEX - STARTINDEX;
    }

    @Override
    public void reduce(IntWritable key, Iterator<Vector> iterator, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {

        double[] sum = new double[DATALENGTH];
        for (int i = 0; i < DATALENGTH; i++) {
            sum[i] = 0;
        }
        
        int count = 0;
        while (iterator.hasNext()) {
            Vector vector = iterator.next();

            for (int i = 0; i < DATALENGTH; i++) {
                sum[i] += vector.getData()[i];
            }
            count++;

            Text text = new Text(vector.toString());
            outputCollector.collect(key, text);
        }


        double[] newCenter = new double[DATALENGTH];
        for (int i = 0; i < DATALENGTH; i++) {
            newCenter[i] = sum[i] / count;
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        List<double[]> curr_center = new ArrayList<>();
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(CURR_CENTER))));
        String line;
        
        while ((line = br.readLine()) != null) {
            String[] split = line.split(",");
            double[] temp = new double[split.length];
            for (int i = 0; i < split.length; i++) {
                temp[i] = Double.parseDouble(split[i]);
            }
            curr_center.add(temp);
        }

        List<String> appendLine = new ArrayList<>();
        if (fs.exists(new Path(NEW_CENTER))) {
            br = new BufferedReader(new InputStreamReader(fs.open(new Path(NEW_CENTER))));
            
            while ((line = br.readLine()) != null) {
                appendLine.add(line);
            }
        }

        PrintWriter pw = new PrintWriter(new OutputStreamWriter(fs.create(new Path(NEW_CENTER), true)));
        for (String string : appendLine) {
            pw.println(string);
            pw.flush();
        }
        
        line = "";
        for (int i = 0; i < DATALENGTH; i++) {
            line += newCenter[i] + ",";
        }
        String substring = line.substring(0, line.length()-1);
        
        pw.println(substring);
        pw.flush();
        pw.close();
        
        double curr_Distance = DistanceComparator.findDistance(curr_center.get(key.get()), newCenter);
        if(curr_Distance < 0.01){
            PrintWriter pw1 = new PrintWriter(new OutputStreamWriter(fs.create(new Path(CENTER_CONVERGED), true)));
            pw1.println("converged");
            pw1.flush();
            pw1.close();
        }

    }
    
}
