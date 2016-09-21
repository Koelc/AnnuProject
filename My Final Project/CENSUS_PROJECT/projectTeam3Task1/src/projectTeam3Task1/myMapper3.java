package projectTeam3Task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class myMapper3 extends Mapper<myKey, myValue, Text, IntWritable>{
	public void map(myKey k,myValue v, Context c) throws IOException, InterruptedException{
		String gender = v.getGender().toString().trim();
		String ms = v.getMStatus().toString().trim();
		if(gender.equals("Female") && (ms.equals("Divorced") || ms.equals("Widowed"))){
			c.write(new Text("The total count of employability to be generated is "), new IntWritable(1));
		}
	}
}
