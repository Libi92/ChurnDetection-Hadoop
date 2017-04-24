package com.chinnu.churndetection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.chinnu.churndetection.distance.DistanceMapper;
import com.chinnu.churndetection.distance.DistanceReducer;
import com.chinnu.churndetection.kmeans.KMeansMapper;
import com.chinnu.churndetection.kmeans.KMeansReducer;
import com.chinnu.churndetection.mewfinder.MewMapper;
import com.chinnu.churndetection.mewfinder.MewReducer;
import com.chinnu.churndetection.mountaincentroids.MountainCentroidMapper;
import com.chinnu.churndetection.mountaincentroids.MountainCentroidReducer;
import com.chinnu.churndetection.mountainfunction.MountainMapper;
import com.chinnu.churndetection.mountainfunction.MountainReducer;
import com.chinnu.churndetection.pi.PiMapper;
import com.chinnu.churndetection.pi.PiReducer;
import com.chinnu.churndetection.utils.Constants;
import com.chinnu.churndetection.utils.MountainModel;
import com.chinnu.churndetection.utils.Vector;

public class ChurnDriver {
	private static final String BASE_DIR = "ChurnDetection/";
	public static final String INPUT_DIR = BASE_DIR + "input/";
	public static final String MEW_OUTPUT_DIR = BASE_DIR + "mew_out/";
	private static final String INPUT_FILE = INPUT_DIR + "data.csv";
	private static final String PI_OUTPUT_DIR = BASE_DIR + "pi_out/";
	private static final String DISTANCE_OUTPUT_DIR = BASE_DIR + "distance_out/";
	private static final String MOUNTAIN_1_OUTPUT_DIR = BASE_DIR + "mountain_out/";
	private static final String MOUNTAIN_CENTROID_OUTPUT_DIR = BASE_DIR + "mountain2_out/";
	private static final String KMEANS_INPUT_DIR = BASE_DIR + "kmeans_in/";
	private static final String KMEANS_INPUT_FILE = KMEANS_INPUT_DIR + "input.csv";
	private static final String KMEANS_CENTROID_DIR = BASE_DIR + "centroid/";
	private static final String KMEANS_CENTROID_FILE = KMEANS_CENTROID_DIR + "centers.txt";
	private static final String KMEANS_OUTPUT_DIR = BASE_DIR + "kmeans_out/";
	private static final String CENTER_CONVERGED = BASE_DIR + "converged.txt";
	private static final String OUT_FILE = "part-r-00000";
	private static final String SEPARATOR = ",";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		delete_dir(conf, MEW_OUTPUT_DIR);
		delete_dir(conf, PI_OUTPUT_DIR);
		delete_dir(conf, DISTANCE_OUTPUT_DIR);
		delete_dir(conf, MOUNTAIN_1_OUTPUT_DIR);
		delete_dir(conf, MOUNTAIN_CENTROID_OUTPUT_DIR);
						
		runJob("MewJob", conf, MewMapper.class, MewReducer.class, LongWritable.class, DoubleWritable.class, INPUT_DIR, MEW_OUTPUT_DIR);
		runJob("PiJob", conf, PiMapper.class, PiReducer.class, IntWritable.class, DoubleWritable.class, MEW_OUTPUT_DIR, PI_OUTPUT_DIR);
		
		
		int firstCentroid = getFirstCentroid(conf);
		System.out.println("First Centroid: " + firstCentroid);
		
		if(firstCentroid != 0) {
			String X1F = getPointAt(conf, firstCentroid);
			System.out.println(X1F);
			if(X1F != null) {
				conf.set(Constants.X1F, X1F);
				runJob("DistanceJob", conf, DistanceMapper.class, DistanceReducer.class, IntWritable.class, DoubleWritable.class, INPUT_DIR, DISTANCE_OUTPUT_DIR);
				double T1 = getT1(conf);
				System.out.println("T1 = " + T1);
				
				if(T1 != 0){
					conf.setDouble(Constants.T1, T1);
					runJob("MountainJob", conf, MountainMapper.class, MountainReducer.class, IntWritable.class, DoubleWritable.class, INPUT_DIR, MOUNTAIN_1_OUTPUT_DIR);
					
					double M1 = getM1(conf);
					System.out.println("M1 = " + M1);
					
					if(M1 != 0) {
						conf.setDouble(Constants.M1, M1);
						runJob("MountainCentroidJob", conf, MountainCentroidMapper.class, MountainCentroidReducer.class, IntWritable.class, DoubleWritable.class, INPUT_DIR, MOUNTAIN_CENTROID_OUTPUT_DIR);
						
						getCentroids(conf, M1);
						createKMeansInput(conf);
						
						runKmeans();
					}
				}
			}
		}else {
			System.out.println("Unable to find first centroid");
		}
	}

	private static void runJob(String jobName, Configuration conf, Class mapperClass, Class reducerClass,
			Class outputKeyClass, Class outputValueClass, String inputDir, String outputDir) {

		try {
			Job job = Job.getInstance(conf, jobName);

			job.setJarByClass(ChurnDriver.class);
			job.setMapperClass(mapperClass);
			job.setReducerClass(reducerClass);

			job.setOutputKeyClass(outputKeyClass);
			job.setOutputValueClass(outputValueClass);

			FileInputFormat.setInputPaths(job, new Path(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));

			if (!job.waitForCompletion(true))
				return;
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
	
	private static void runKmeans() {
		try {

            int iterations = 1;
            Path convergerPath = new Path(CENTER_CONVERGED);
            Path centerPath = new Path(KMEANS_CENTROID_FILE);
            Path nextCenterPath = new Path(KMEANS_CENTROID_DIR + "centers" + (iterations + 1) + ".txt");

            JobConf conf = new JobConf(ChurnDriver.class);

            Path outPath = new Path(KMEANS_OUTPUT_DIR);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            if (fs.exists(convergerPath)) {
                fs.delete(convergerPath, true);
            }
            
            FileStatus[] fss = fs.listStatus(new Path(KMEANS_CENTROID_DIR));
            for (FileStatus status : fss) {
                Path path = status.getPath();
                if (path.toString().contains("centers.txt")) {
                    continue;
                }
                fs.delete(path);
            }

            conf.setJobName("KMEANS_" + iterations);
            conf.setMapOutputKeyClass(IntWritable.class);
            conf.setMapOutputValueClass(Vector.class);
            conf.setOutputKeyClass(IntWritable.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(KMeansMapper.class);
            conf.setReducerClass(KMeansReducer.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            conf.set(Constants.CENTER, centerPath.toString());
            conf.set(Constants.NEXTCENTER, nextCenterPath.toString());
            conf.setInt(Constants.STARTINDEX, 1);
            conf.setInt(Constants.ENDINDEX, 4);
            conf.setInt(Constants.CLASSINDEX, 5);

            Job job = Job.getInstance(conf);
            FileInputFormat.setInputPaths(job, new Path(KMEANS_INPUT_DIR));
            FileOutputFormat.setOutputPath(job, new Path(KMEANS_OUTPUT_DIR));

            JobClient.runJob(conf);

            while (true) {

                System.out.println("------CENTERS------");

                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(nextCenterPath)));
                String line;
                line = br.readLine();
                while (line != null) {
                    System.out.println(line);
                    line = br.readLine();
                }
                iterations++;

                centerPath = new Path(KMEANS_CENTROID_DIR + "centers" + iterations + ".txt");
                nextCenterPath = new Path(KMEANS_CENTROID_DIR + "centers" + (iterations + 1) + ".txt");

                conf = new JobConf(ChurnDriver.class);

                outPath = new Path(KMEANS_OUTPUT_DIR);
                fs = FileSystem.get(conf);
                if (fs.exists(outPath)) {
                    fs.delete(outPath, true);
                }

                conf.setJobName("KMEANS_" + iterations);
                conf.setMapOutputKeyClass(IntWritable.class);
                conf.setMapOutputValueClass(Vector.class);
                conf.setOutputKeyClass(IntWritable.class);
                conf.setOutputValueClass(Text.class);
                conf.setMapperClass(KMeansMapper.class);
                conf.setReducerClass(KMeansReducer.class);
                conf.setInputFormat(TextInputFormat.class);
                conf.setOutputFormat(TextOutputFormat.class);
                conf.set(Constants.CENTER, centerPath.toString());
                conf.set(Constants.NEXTCENTER, nextCenterPath.toString());
                conf.setInt(Constants.STARTINDEX, 1);
                conf.setInt(Constants.ENDINDEX, 4);
                conf.setInt(Constants.CLASSINDEX, 5);

                Job jobi = Job.getInstance(conf);
                FileInputFormat.setInputPaths(jobi, new Path(KMEANS_INPUT_DIR));
                FileOutputFormat.setOutputPath(jobi, new Path(KMEANS_OUTPUT_DIR));

                JobClient.runJob(conf);
                
                if(fs.exists(convergerPath)){
                    break;
                }
            }

            fss = fs.listStatus(new Path(KMEANS_OUTPUT_DIR));
            for (FileStatus status : fss) {
                Path path = status.getPath();
                if (path.toString().contains("_SUCCESS")) {
                    continue;
                }

                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                line = br.readLine();
                while (line != null) {
                    System.out.println(line);
                    line = br.readLine();
                }

            }

            System.out.println("------CENTERS------");

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centerPath)));
            String line;
            line = br.readLine();
            while (line != null) {
                System.out.println(line);
                line = br.readLine();
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        }
	}
	
	private static void createKMeansInput(Configuration conf){
		delete_dir(conf, KMEANS_INPUT_FILE);
		
		FileSystem fileSystem;
		try {
			fileSystem = FileSystem.get(conf);
			Path inputPath = new Path(INPUT_FILE);
			BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(inputPath)));
			Path kmeansInPath = new Path(KMEANS_INPUT_FILE);
			FSDataOutputStream outputStream = fileSystem.create(kmeansInPath, true);
			OutputStreamWriter streamWriter = new OutputStreamWriter(outputStream);
			PrintWriter pw = new PrintWriter(streamWriter);
			String line;
			while ((line = br.readLine()) != null) {
				String[] split = line.split(",");
				
				String data = split[Constants.ITEM_INDEX] + SEPARATOR + 
						split[Constants.DAY_CALL_INDEX] + SEPARATOR + 
						split[Constants.EVE_CALL_INDEX] + SEPARATOR +
						split[Constants.NIGHT_CALL_INDEX] + SEPARATOR + 
						split[Constants.INTR_CALL_INDEX] + SEPARATOR +
						split[Constants.CLASS_INDEX];
				
				pw.println(data);
				pw.flush();
			}
			pw.close();
			
			
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}
	
	private static void getCentroids(Configuration conf, double M1){
		FileSystem fileSystem;
		try {
			fileSystem = FileSystem.get(conf);
			Path inputPath = new Path(MOUNTAIN_CENTROID_OUTPUT_DIR + OUT_FILE);
			BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(inputPath)));

			String line;
			List<MountainModel> mountains = new ArrayList<>();
			while ((line = br.readLine()) != null) {
				String[] split = line.split("\t");
				int xi = Integer.parseInt(split[0]);
				double mi = Double.parseDouble(split[1]);

				MountainModel model = new MountainModel();
				model.setXi(xi);
				model.setMi(mi);
				
				mountains.add(model);
			}
			
			Collections.sort(mountains);
			
			List<Integer> centroids = new ArrayList<>();
			for (MountainModel model : mountains) {
				double mi = 0.6 * model.getMi();
				if (mi > M1) {
					centroids.add(model.getXi());
				}
				else{
					break;
				}
			}
			
			delete_dir(conf, KMEANS_CENTROID_FILE);
			
			Path kmeansInPath = new Path(KMEANS_CENTROID_FILE);
			FSDataOutputStream outputStream = fileSystem.create(kmeansInPath, true);
			OutputStreamWriter streamWriter = new OutputStreamWriter(outputStream);
			PrintWriter pw = new PrintWriter(streamWriter);
			
			System.out.println("----- Centroids -----");
			for (Integer xi : centroids) {
				System.out.print(xi + ", ");
				
				String point = getPointAt(conf, xi);
				String[] split = point.split(",");
				
				String data = split[Constants.DAY_CALL_INDEX] + SEPARATOR + 
						split[Constants.EVE_CALL_INDEX] + SEPARATOR +
						split[Constants.NIGHT_CALL_INDEX] + SEPARATOR + 
						split[Constants.INTR_CALL_INDEX];
				
				pw.println(data);
				pw.flush();
			}
			pw.close();

			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static int getFirstCentroid(Configuration conf) {
		FileSystem fileSystem;
		try {
			fileSystem = FileSystem.get(conf);
			Path inputPath = new Path(PI_OUTPUT_DIR + OUT_FILE);
			BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(inputPath)));

			String line;
			double minPi = Double.MAX_VALUE;
			int centroid = 1;
			while ((line = br.readLine()) != null) {
				String[] split = line.split("\t");
				int xi = Integer.parseInt(split[0]);
				double pi = Double.parseDouble(split[1]);

				if (pi < minPi) {
					minPi = pi;
					centroid = xi;
				}
			}
			return centroid;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

	private static double getT1(Configuration conf) {
		FileSystem fileSystem;
		try {
			fileSystem = FileSystem.get(conf);
			Path inputPath = new Path(DISTANCE_OUTPUT_DIR + OUT_FILE);
			BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(inputPath)));

			String line = br.readLine();
			if (line != null) {
				double T1 = Double.parseDouble(line.split("\t")[1]);
				return T1;
			}

			return 0;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

	private static double getM1(Configuration conf) {
		FileSystem fileSystem;
		try {
			fileSystem = FileSystem.get(conf);
			Path inputPath = new Path(MOUNTAIN_1_OUTPUT_DIR + OUT_FILE);
			BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(inputPath)));

			String line = br.readLine();
			if (line != null) {
				double M1 = Double.parseDouble(line.split("\t")[1]);
				return M1;
			}

			return 0;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

	private static String getPointAt(Configuration conf, int x1) {
		FileSystem fileSystem;
		try {
			fileSystem = FileSystem.get(conf);
			Path inputPath = new Path(INPUT_FILE);
			BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(inputPath)));

			String line;
			while ((line = br.readLine()) != null) {
				String[] split = line.split(",");

				if (split[0].equals(x1 + "")) {
					return line;
				}
			}
			return null;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static void delete_dir(Configuration conf, String dir) {
		Path outPath = new Path(dir);
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			if (fs.exists(outPath)) {
				fs.delete(outPath, true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
