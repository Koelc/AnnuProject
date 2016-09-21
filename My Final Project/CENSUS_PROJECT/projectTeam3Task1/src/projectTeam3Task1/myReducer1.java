package projectTeam3Task1;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	public void reduce(Text rKey, Iterable<DoubleWritable> rVal, Context c) throws IOException, InterruptedException{
		double total = 0.0;
		for (DoubleWritable doubleWritable : rVal) {
			total+= doubleWritable.get();
		}
		c.write(new Text("The total amount of pension will be "), new DoubleWritable(total));
	}
	
	
}
