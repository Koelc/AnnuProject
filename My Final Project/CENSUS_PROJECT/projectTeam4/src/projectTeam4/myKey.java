package projectTeam4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class myKey implements WritableComparable {
	
	Text age;
	
	public myKey(){
		age = new Text();
	}
	
	public myKey(Text age){
		this.age = age;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		age.readFields(arg0);
	}

	@Override
	public void write(DataOutput out) throws IOException{
		age.write(out);
	}

	public int compareTo(Object o){
		myKey other = (myKey)o;
		return age.compareTo(other.age);
	}
	public String getage() {
		return age.toString();
	}
	public void setage(Text age) {
		this.age = age;
	}

}
