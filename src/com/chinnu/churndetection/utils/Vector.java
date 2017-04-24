/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.chinnu.churndetection.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author libin
 */
public class Vector implements Writable {

    int Index;
    int dataLength;
    double[] data;
    String className;

    public int getIndex() {
        return Index;
    }

    public void setIndex(int Index) {
        this.Index = Index;
    }

    public int getDataLength() {
        return dataLength;
    }

    public void setDataLength(int dataLength) {
        this.dataLength = dataLength;
    }

    public double[] getData() {
        return data;
    }

    public void setData(double[] data) {
        this.dataLength = data.length;
        this.data = data;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public static Vector read(DataInput in) throws IOException {
        Vector v = new Vector();
        v.readFields(in);
        return v;
    }

    @Override
    public void write(DataOutput out) {
        try {
            out.writeInt(Index);
            out.writeInt(dataLength);

            for (int i = 0; i < dataLength; i++) {
                out.writeDouble(data[i]);
            }

            out.writeChars(className);
        } catch (IOException ex) {
            Logger.getLogger(Vector.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void readFields(DataInput in) {
        try {
            Index = in.readInt();
            dataLength = in.readInt();
            data = new double[dataLength];

            for (int i = 0; i < dataLength; i++) {
                data[i] = in.readDouble();
            }

            className = in.readLine();
        } catch (IOException ex) {
            Logger.getLogger(Vector.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Override
    public String toString() {
        return Index + "," + className;
    }

}
