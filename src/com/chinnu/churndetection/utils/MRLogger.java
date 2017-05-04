package com.chinnu.churndetection.utils;

import java.io.FileWriter;
import java.io.IOException;

public class MRLogger {
	private static boolean logEnabled = true;

	public static void Log(String text){
		if(logEnabled){
			String filename= "/Users/libin/Documents/mr_log.txt";
	        FileWriter fw;
			try {
				fw = new FileWriter(filename,true);
				fw.write(text + "\n");
		        fw.close();
			} catch (IOException e) {	
				e.printStackTrace();
			}
	        
		}
	}
}
