package projectTeam3Task1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class myValue implements Writable {
	
	Text gender,maritalStatus,income,parents;
	
	public myValue(){
	gender = new Text();	
	maritalStatus = new Text();
	income = new Text();
	parents = new Text();
	}
	
	public myValue(Text gender,Text maritalStatus,Text income,Text parents){
		this.gender = gender;	
		this.maritalStatus = maritalStatus;
		this.income = income;
		this.parents = parents;
		}
	
	

	@Override
	public void readFields(DataInput arg0) throws IOException {
		gender.readFields(arg0);
		maritalStatus.readFields(arg0);
		income.readFields(arg0);
		parents.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		gender.write(arg0);
		maritalStatus.write(arg0);
		income.write(arg0);
		parents.write(arg0);
	}
	
	public Text getGender(){
		return gender;
	}
	
	public Text getMStatus(){
		return maritalStatus;
	}
	
	public Text getIncome(){
		return income;
	}
	
	public Text getParents(){
		return parents;
	}
	
	public void setGender(Text gender){
		this.gender = gender;
	}
	
	public void setMStatus(Text maritalStatus){
		this.maritalStatus = maritalStatus;
	}
	
	public void setIncome(Text income){
		this.income = income;
	}
	
	public void setParents(Text parents){
		this.parents = parents;
	}

}
