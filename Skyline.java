package Skyline.COMP406_Project;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
* This Skyline program implements an application that
* detects the Skyline points in a csv data set for 
* house rental with columns id,price,age and
* distance from city center.
* 
* args[0] = Name of the main Class "Skyline"
* 
* args[1] = Name of the input.csv file
* 
* The input file must be in the input folder
* in the HDFS 
* 
* args[2] = Name of the output.csv file 
* 
* The output will be created in the same folder 
* that the jar is  
* 
* args[3] = #of partitions to the data set
* 
* args[4] = partitioning method random|angle
* 
* args[5] = dimentions to take into account 2d|3d 
* {2d takes account only price and age}
* 
* Note: HDFS must be running in the port "127.0.0.1:9000"
*
* @author  Kyriakos Psarakis 	 
* @date   03/05/2017 (dd/mm/yyyy)
*/

public class Skyline extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		
		/*
		 * Input and output paths
		 */
		
		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/"+args[1]);
		Path outputPath1 = new Path("hdfs://127.0.0.1:9000/output/First_Run");
		Path outputPath2 = new Path("hdfs://127.0.0.1:9000/output/Final_Result");
		
		/*
		 *  HDFS configuration 
		 */
		
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"),getConf());
		
		if (hdfs.exists(outputPath1))
			hdfs.delete(outputPath1, true);
		
		if (hdfs.exists(outputPath2))
			hdfs.delete(outputPath2, true);
		
		/*
		 * Local Skyline Job
		 */

	    Job job1 = createJob("Local Skylines",Integer.parseInt(args[3]),args[4],args[5],inputPath,outputPath1);

		job1.waitForCompletion(true);
		
		/*
		 * Global Skyline Job
		 */
		
		Job job2 = createJob("Global Skylines",1,"random",args[5],outputPath1,outputPath2);
		
		int res = job2.waitForCompletion(true)? 0 : 1;
		
		/*
		 * Create the output csv file 
		 */

		createOutputCSV(args[2],hdfs);
		
		return res;

	}
	
	public Job createJob(String jName,int N,String pM,String D,Path in,Path out) throws IOException{
		
		// create the job instance 
		Job job = Job.getInstance(getConf(),jName);
		
		// set the jar class 
		job.setJarByClass(Skyline.class);
		
		// set Map Combiner and Reduce classes 
		job.setMapperClass(SkylineMapper.class);
		job.setCombinerClass(SkylineCombiner.class);
		job.setReducerClass(SkylineReducer.class);
	    
		// set Map and Reduce I/O classes 
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		// set I/O format 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// set global parameters 
		job.getConfiguration().setInt("NuberOfPartitions",N);
		job.getConfiguration().set("PartitioningMethod",pM);
		job.getConfiguration().set("Dimensions",D);
		
		// set I/O paths 
		TextInputFormat.addInputPath(job,in);
		TextOutputFormat.setOutputPath(job,out);
		
		return job;
		
	}
	
	public void createOutputCSV(String name,FileSystem hdfs) throws IOException{
		
		Path pt = new Path("hdfs://127.0.0.1:9000/output/Final_Result/part-r-00000");
		
		PrintWriter writer = new PrintWriter(name);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(pt)));
		
		writer.println("ApartmentId , Price, Age, DistanceFromCityCenter");

		String line;
		
		line = br.readLine();
		
		while (line != null) {
			writer.println(line);
			line = br.readLine();
		}
		
		System.out.println("\nOutput File Created Successfully !!!");
		
		br.close();
	    writer.close();
	    hdfs.close();
	    
	}
	
	public static void main(String[] args) throws Exception {
		
		/*
		 * Input arguments check
		 */
		
		if(args.length!=6){
			throw new java.lang.Error("Invalid Number of args they must be 6\n{main class name} {input.csv name} {output.csv name} {Number of Partitions} {Partitioning Method} {Dimentions}");
		}
		
		int N = Integer.parseInt(args[3]);
		
		if(N<1){
			throw new java.lang.Error("Invalid Number of Partitions");
		}
		
		if(!((args[4].equals("random"))||(args[4].equals("angle")))){
			throw new java.lang.Error("Invalid Partitioning Method");
		}
		
		if(!((args[5].equals("2d"))||(args[5].equals("3d")))){
			throw new java.lang.Error("Invalid Dimention Number");
		}
		
		/*
		 * Run the map-reduce jobs 
		 */
	
		ToolRunner.run(new Configuration(), new Skyline(), args);
	}

}
