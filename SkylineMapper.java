package Skyline.COMP406_Project;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SkylineMapper extends  Mapper<Object, Text, IntWritable, Text> {

	private Text v = new Text();
	private int N;
	private String M, D;

	public void map(Object key, Text value,Context context) throws IOException, InterruptedException {

		String line = value.toString();
		int k = -1;
		
		// if its not the first line of the csv file that starts with an {A} "ApartmentID"
		if (line.charAt(0) != 'A') {
			// if we have random partitioning
			if (M.equals("random")) {
				k = ThreadLocalRandom.current().nextInt(1, N + 1);
			} else {
			// if we have angle based partitioning
				if (D.equals("2d")) {
					// if we have 2D partitioning
					k = angleKey2D(angleCalc2D(line));
				} else {
					// if we have 3D partitioning
					k = angleKey3D(angleCalc3D(line), angleCalc2D(line));
				}
			}

			v.set(line);
			
			context.write(new IntWritable(k),v);

		}

	}
	
	/*
	 *  Take the Configuration parameters
	 */

	public void setup(Context context) throws IOException,InterruptedException {
		
		N = context.getConfiguration().getInt("NuberOfPartitions",1);
		M = context.getConfiguration().get("PartitioningMethod");
		D = context.getConfiguration().get("Dimensions");
 
	}
	
	/*
	 * Create the key for 2D taking into account that we only have positive points in the fist quadrant
	 */
	
	public int angleKey2D(double angle) {

		double area = 90.0000;
		double step = area / N;

		return (int) Math.ceil(angle / step);

	}
	
	
	/*
	 * Angle calculations for 2D & 3D 
	 */

	public double angleCalc2D(String line) {

		SkylineCandidate point = new SkylineCandidate(line);

		return Math.toDegrees(Math.atan(point.getAge() / point.getPrice()));

	}

	public double angleCalc3D(String line) {

		SkylineCandidate point = new SkylineCandidate(line);

		double r = Math.sqrt(Math.pow(point.getPrice(), 2)+ Math.pow(point.getAge(), 2)+ Math.pow(point.getDistance(), 2));

		return Math.toDegrees(Math.acos(point.getDistance() / r));

	}

	/*
	 * Create the key for 3D taking into account that we only have positive points in the fist quadrant
	 */
	
	public int angleKey3D(double theta, double phi) {

		if ((theta > 0) && (theta <= 30)) {
			if ((phi > 0) && (phi <= 48.24)) {
				return 1;
			} else if ((phi > 48.24) && (phi <= 70.52)) {
				return 2;
			} else if ((phi > 70.52) && (phi <= 90)) {
				return 3;
			}
		} else if ((theta > 30) && (theta <= 60)) {
			if ((phi > 0) && (phi <= 48.24)) {
				return 4;
			} else if ((phi > 48.24) && (phi <= 70.52)) {
				return 5;
			} else if ((phi > 70.52) && (phi <= 90)) {
				return 6;
			}
		} else if ((theta > 60) && (theta <= 90)) {
			if ((phi > 0) && (phi <= 48.24)) {
				return 7;
			} else if ((phi > 48.24) && (phi <= 70.52)) {
				return 8;
			} else if ((phi > 70.52) && (phi <= 90)) {
				return 9;
			}
		}

		System.out.println("Something wrong happend in the 3d partitioning !!!");
		return -1;

	}

}