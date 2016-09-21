package projectTeam2;

public class secKey {
	double min, max;
	String gen;
	
	secKey(double min, double max, String gen){
		this.min=min;
		this.max = max;
		this.gen= gen;
	}
	
	void setMin(double min){
		this.min=min;
	}
	
	void setMax(double max){
		this.max=max;
	}
	
	void setGen(String gen){
		this.gen= gen;
	}
	
	double getMin(){
		return min;
	}
	
	double getMax(){
		return max;
	}
	
	String getGen(){
		return gen;
	}

}
