package projectTeam3Task1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text k, Iterable<DoubleWritable> v, Context c) throws IOException, InterruptedException{
		double total=0;
		for(DoubleWritable each:v){
			total+=each.get();
		}
		c.write(new Text("Total Scholarship amount is :"), new DoubleWritable(total));
	}

}
