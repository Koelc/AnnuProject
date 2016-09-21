package projectTeam4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class myMapper1 extends Mapper<myKey, myValue, Text, IntWritable>{
	public void map(myKey k, myValue v, Context c) throws IOException, InterruptedException{
		int year = c.getConfiguration().getInt("years", 0);
		int age = Integer.parseInt(k.getage());
		if((age+year)>=18){
			c.write(new Text("Number of Voters after " +year + " will be : "), new IntWritable(1));
		}
	}

}
