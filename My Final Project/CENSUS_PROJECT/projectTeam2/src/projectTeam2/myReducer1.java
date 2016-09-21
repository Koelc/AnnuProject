package projectTeam2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer1 extends Reducer<Text, Text, Text, Text>{
	double income=0;
	double tamt;
	int taxpayer=0;
	public void reduce(Text k, Iterable<Text> v, Context c) throws IOException, InterruptedException{
		for(Text each : v){
			String[] eachVal=each.toString().split(",");
			income+= Double.parseDouble(eachVal[1]);
			tamt+=Double.parseDouble(eachVal[0]);
			taxpayer++;
		}
		c.write(new Text("The total number of tax payers are : " +taxpayer), new Text(" , the total income of all the tax payer is : "+income +" the total amount of tax that will be generated is : " +tamt));
	}

}
