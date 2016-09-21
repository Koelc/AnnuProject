package projectTeam4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class myMapper2 extends Mapper<myKey, myValue, Text, IntWritable> {
	public void map(myKey k , myValue v, Context c) throws IOException, InterruptedException{
		int year = c.getConfiguration().getInt("years", 0);
		int srAge = c.getConfiguration().getInt("age", 0);
		int age = Integer.parseInt(k.getage());
		if((age+year)>=srAge){
			c.write(new Text("Number of senior citizen after " +year + " years will be : "), new IntWritable(1));
		}
	}

}
