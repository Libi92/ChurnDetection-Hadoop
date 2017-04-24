package com.chinnu.churndetection.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MountainWritable implements Writable {
	
	private double exp;
	private double distance;

	
	public double getExp() {
		return exp;
	}

	public void setExp(double exp) {
		this.exp = exp;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}
	
	public static MountainWritable read(DataInput in) throws IOException {
		MountainWritable writable = new MountainWritable();
		writable.readFields(in);
		return writable;
	}

	@Override
	public void readFields(DataInput in) {
		try {
			exp = in.readDouble();
			distance = in.readDouble();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(DataOutput out) {
		try {
			out.writeDouble(exp);
			out.writeDouble(distance);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
