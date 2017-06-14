package Skyline.COMP406_Project;

public class SkylineCandidate {

	// Skyline point columns for house rental 
	private int id;
	private double price,age,distance;
	
	
	public SkylineCandidate(String Skyline){
		
		// Take them from a csv format and split them 
		String[] Words = Skyline.split(" , ");
		
		id = Integer.parseInt(Words[0]);
		price = Double.parseDouble(Words[1]);
		age = Double.parseDouble(Words[2]);
		distance = Double.parseDouble(Words[3]);
	
	}

	// Object Getters 
	
	public String getPointString(){
		return this.id + " , "+ this.price + " , " + this.age + " , " + this.distance;
	}
	
	public int getId() {
		return id;
	}


	public double getPrice() {
		return price;
	}


	public double getAge() {
		return age;
	}


	public double getDistance() {
		return distance;
	}

}
