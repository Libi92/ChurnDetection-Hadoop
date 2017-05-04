/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.chinnu.churndetection.kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;

import com.chinnu.churndetection.ChurnDriver;
import com.chinnu.churndetection.utils.Constants;
import com.chinnu.churndetection.utils.DistanceComparator;
import com.chinnu.churndetection.utils.MRLogger;
import com.chinnu.churndetection.utils.Vector;

/**
 *
 * @author libin
 */
public class KMeansReducer extends Reducer<IntWritable, Vector, IntWritable, Text> {

    String NEW_CENTER;
    String CURR_CENTER;
    int STARTINDEX;
    int ENDINDEX;
    int DATALENGTH;
    
    @Override
    protected void setup(Reducer<IntWritable, Vector, IntWritable, Text>.Context context)
    		throws IOException, InterruptedException {
    	
    	Configuration conf = context.getConfiguration();
    	
    	NEW_CENTER = conf.get(Constants.NEXTCENTER);
        CURR_CENTER = conf.get(Constants.CENTER_TEXT);
        STARTINDEX = conf.getInt(Constants.STARTINDEX, 0);
        ENDINDEX = conf.getInt(Constants.ENDINDEX, 0);
        DATALENGTH = ENDINDEX - STARTINDEX;
        
    	super.setup(context);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Vector> values,
    		Reducer<IntWritable, Vector, IntWritable, Text>.Context context) throws IOException, InterruptedException {
    	
        double[] sum = new double[DATALENGTH];
        for (int i = 0; i < DATALENGTH; i++) {
            sum[i] = 0;
        }
        
        int count = 0;
        for(Vector vector : values){

            for (int i = 0; i < DATALENGTH; i++) {
                sum[i] += vector.getData()[i];
            }
            count++;

            Text text = new Text(vector.toString());
            context.write(key, text);
        }


        double[] newCenter = new double[DATALENGTH];
        for (int i = 0; i < DATALENGTH; i++) {
            newCenter[i] = sum[i] / count;
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        List<double[]> curr_center = new ArrayList<>();
        
        String[] lineSplit = CURR_CENTER.split("\n");
        for(int j = 0; j < lineSplit.length; j++) {
        	String line = lineSplit[j];
        	String[] split = line.split(",");
            double[] temp = new double[split.length];
            for (int i = 0; i < split.length; i++) {
                temp[i] = Double.parseDouble(split[i]);
            }
            curr_center.add(temp);
        }

        List<String> appendLine = new ArrayList<>();
        if (fs.exists(new Path(NEW_CENTER))) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(NEW_CENTER))));
            
            String line;
            while ((line = br.readLine()) != null) {
                appendLine.add(line);
            }
        }

        PrintWriter pw = new PrintWriter(new OutputStreamWriter(fs.create(new Path(NEW_CENTER), true)));
        for (String string : appendLine) {
            pw.println(string);
            pw.flush();
        }
        
        String line = "";
        for (int i = 0; i < DATALENGTH; i++) {
            line += newCenter[i] + ",";
        }
        String substring = line.substring(0, line.length()-1);
        
        pw.println(substring);
        pw.flush();
        pw.close();
        
        MRLogger.Log(context.getJobName());
        MRLogger.Log(Arrays.toString(curr_center.get(key.get())));
        MRLogger.Log(Arrays.toString(newCenter));
        
        double curr_Distance = DistanceComparator.findDistance(curr_center.get(key.get()), newCenter);
        MRLogger.Log(curr_Distance + "");
        
        if(curr_Distance < 0.01){
            PrintWriter pw1 = new PrintWriter(new OutputStreamWriter(fs.create(new Path(ChurnDriver.CENTER_CONVERGED), true)));
            pw1.println("converged");
            pw1.flush();
            pw1.close();
        }

    }
    
}
