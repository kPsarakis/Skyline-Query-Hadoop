package Skyline.COMP406_Project;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SkylineCombiner extends Reducer<IntWritable, Text, IntWritable, Text> { 

	private Text v = new Text();
	private String D;

	/*
	 * Reducer implements BNL algorithm
	 */
	
	
	public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {

		Vector<String> Window = new Vector<String>();
		SkylineCandidate curPoint, tempPoint;
		String curVal;
		boolean flag;

		//Outer loop over all the points 
		for(Text val:values){
			
			curVal = val.toString();

			curPoint = new SkylineCandidate(curVal);

			if (Window.isEmpty()) {

				Window.addElement(curPoint.getPointString());
				curVal = new String();

			} else {

				flag = true;

				// Inner loop over the window 
				for (int i = 0; i < Window.size(); i++) {

					tempPoint = new SkylineCandidate(Window.get(i));

					// CurValue dominates window element
					if (Dominates(curPoint, tempPoint, D)) {

						Window.remove(i);
						i--;
						
					}

					// CurValue dominated
					if (isDominated(curPoint, tempPoint, D)) {

						flag = false;
						break;

					}

				}

				// CurValue Survived the inner loop
				if (flag == true) {

					Window.addElement(curPoint.getPointString());
					curVal = new String();

				}
			}
		}

		// After we finish with the BNL algorithm we write the contents of the window as results
		
		for (int i = 0; i < Window.size(); i++) {
			v.set(Window.get(i));
			context.write(key,v);
		}

	}

	/*
	 * Check if a point gets dominated  
	 */

	public boolean isDominated(SkylineCandidate cP, SkylineCandidate tP,String dim) {

		if (dim.equals("2d")) {
			if ((tP.getPrice() <= cP.getPrice()) && (tP.getAge() <= cP.getAge())) {
				return true;
			}
		} else {
			if ((tP.getPrice() <= cP.getPrice()) && (tP.getAge() <= cP.getAge()) && (tP.getDistance() <= cP.getDistance())) {
				return true;
			}
		}

		return false;
	}
	
	/*
	 * Check if a point dominates an other 
	 */

	public boolean Dominates(SkylineCandidate cP, SkylineCandidate tP,String dim) {

		if (dim.equals("2d")) {
			if ((tP.getPrice() >= cP.getPrice()) && (tP.getAge() >= cP.getAge())) {
				return true;
			}
		} else {
			if ((tP.getPrice() >= cP.getPrice()) && (tP.getAge() >= cP.getAge()) && (tP.getDistance() >= cP.getDistance())) {
				return true;
			}
		}

		return false;
	}

	/*
	 *  Take the Configuration parameters
	 */
	
	public void setup(Context context) throws IOException,InterruptedException {
		
		D = context.getConfiguration().get("Dimensions");
 
	}
	
}
