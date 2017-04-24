/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.chinnu.churndetection.utils;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author libin
 */
public class DistanceComparator {

    public static int findMinimumDistance(double[] data, HashMap<Integer, double[]> centers) {

        int minKey = 0;
        double minValue = Double.MAX_VALUE;
        for (Map.Entry<Integer, double[]> entry : centers.entrySet()) {
            Integer key = entry.getKey();
            double[] value = entry.getValue();

            double sum = 0;
            for (int i = 0; i < data.length; i++) {
                sum += Math.pow((data[i] - value[i]), 2);
            }

            double distance = Math.sqrt(sum);

            if (distance < minValue) {
                minKey = key;
                minValue = distance;
            }
        }

        return minKey;
    }

    public static double findDistance(double[] data1, double[] data2) {
        double sum = 0;
        for (int i = 0; i < data1.length; i++) {
            sum += Math.pow((data1[i] - data2[i]), 2);
        }

        double distance = Math.sqrt(sum);
        
        return distance;
    }
}
