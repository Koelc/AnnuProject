package projectTeam2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class myValue implements Writable {
	
	Text education,taxfiler,gender,maritalStatus,income,parents,cob,citizenshp,ww;
	
	public myValue(){
		education = new Text();
		taxfiler = new Text();
	gender = new Text();	
	maritalStatus = new Text();
	income = new Text();
	parents = new Text();
	cob = new Text();
	citizenshp = new Text();
	ww = new Text();
	}
	
	public myValue(Text education, Text taxfiler,Text gender,Text maritalStatus,Text income,Text parents, Text cob, Text citizenshp,Text ww){
		this.education = education;	
		this.taxfiler = taxfiler;	
		this.gender = gender;	
		this.maritalStatus = maritalStatus;
		this.income = income;
		this.parents = parents;
		this.cob = cob;
		this.citizenshp = citizenshp;
		this.ww = ww;
		}
	
	

	@Override
	public void readFields(DataInput arg0) throws IOException {
		education.readFields(arg0);
		taxfiler.readFields(arg0);
		gender.readFields(arg0);
		maritalStatus.readFields(arg0);
		income.readFields(arg0);
		parents.readFields(arg0);
		cob.readFields(arg0);
		citizenshp.readFields(arg0);
		ww.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		education.write(arg0);
		taxfiler.write(arg0);
		gender.write(arg0);
		maritalStatus.write(arg0);
		income.write(arg0);
		parents.write(arg0);
		cob.write(arg0);
		citizenshp.write(arg0);
		ww.write(arg0);
	}
	
	public Text getEducation(){
		return education;
	}
	
	public Text getTaxFiler(){
		return taxfiler;
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
	public Text getCob(){
		return cob;
	}
	public Text getCitiznshp(){
		return citizenshp;
	}
	public Text getWW(){
		return ww;
	}
	
	public void setEducation(Text education){
		this.education = education;
	}
	
	public void setTaxfiler(Text taxfiler){
		this.taxfiler = taxfiler;
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
	
	public void setCob(Text cob){
		this.cob = cob;
	}
	
	public void setCitiznshp(Text citizenshp){
		this.citizenshp = citizenshp;
	}
	
	public void setWW(Text ww){
		this.ww = ww;
	}

}

