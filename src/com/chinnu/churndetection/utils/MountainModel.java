package com.chinnu.churndetection.utils;

public class MountainModel implements Comparable<MountainModel> {
	private int xi;
	private double mi;
	public int getXi() {
		return xi;
	}
	public void setXi(int xi) {
		this.xi = xi;
	}
	public double getMi() {
		return mi;
	}
	public void setMi(double mi) {
		this.mi = mi;
	}
	@Override
	public int compareTo(MountainModel o) {
		if(this.mi > o.mi) {
			return 1;
		} else {
			return -1;
		}
	}
}
