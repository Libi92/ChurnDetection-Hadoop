package com.chinnu.churndetection.builder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.chinnu.churndetection.utils.MountainModel;

public class DataBuilder {
	public static void main(String[] args) {
		File file = new File("input/input.csv");
		File outFile = new File("input/data.csv");
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			PrintWriter pw = new PrintWriter(new FileWriter(outFile));
			String line = br.readLine();
			int i = 1;
			while((line = br.readLine()) != null){
				pw.println(i++ + "," + line);
				pw.flush();
			}
			
			pw.close();
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
