package projectTeam3Task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer3 extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text k, Iterable<IntWritable> v, Context c) throws IOException, InterruptedException{
		int count=0;
		for(IntWritable x: v)
			count++;
		c.write(k, new IntWritable(count));
	}

}
