/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.chinnu.churndetection.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author libin
 */
public class InsertIndex {
    public static void main(String[] args) {
        BufferedReader br = null;
        PrintWriter pw;
        try {
            String path1 = new String("/Users/libin/Documents/Projects/kmeansmapreduce/datasets/iris.data.txt");
            String path2 = new String("/Users/libin/Documents/Projects/kmeansmapreduce/datasets/iris2.data.txt");
            br = new BufferedReader(new FileReader(new File(path1)));
            pw = new PrintWriter(new File(path2));
            String line;
            int index = 1;
            while ((line = br.readLine()) != null) {                
                line = index++ + "," + line;
                pw.println(line);
                pw.flush();
            }
            pw.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(InsertIndex.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(InsertIndex.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                br.close();
            } catch (IOException ex) {
                Logger.getLogger(InsertIndex.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
