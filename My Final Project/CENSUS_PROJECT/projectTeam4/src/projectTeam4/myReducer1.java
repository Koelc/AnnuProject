package projectTeam4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
	int count=0;
	public void reduce(Text k , Iterable<IntWritable> v , Context c) throws IOException, InterruptedException{
		for(IntWritable x: v)
			count++;
			c.write(k, new IntWritable(count));
	}

}
