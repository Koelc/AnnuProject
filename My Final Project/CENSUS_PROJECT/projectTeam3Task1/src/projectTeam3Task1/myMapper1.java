package projectTeam3Task1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class myMapper1 extends Mapper<myKey, myValue, Text, DoubleWritable> {
private HashMap<String, String> pensiondata = new HashMap<String, String>();		
	
	protected void setup(Context context) throws java.io.IOException, InterruptedException{
		
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());		
		for (Path SinglePath : files) {
			if (SinglePath.getName().equals("part-m-00000")) 
			{
	BufferedReader br = new BufferedReader(new FileReader(SinglePath.toString()));
				String line = br.readLine();
				while(line != null) 
				{
					String[] lineParts = line.split(",");
					String prange  = lineParts[0];
					String per_pension = lineParts[1];
					pensiondata.put(prange, per_pension);
					line = br.readLine();
				}
				br.close();
			}
		}
		if (pensiondata.isEmpty()) 
		{
			System.out.println("Unable To Load module Data.");
		}
	}
	
    protected void map(myKey key, myValue value, Context context)
        throws java.io.IOException, InterruptedException 
	{
		 

		double pension=0;
		double sal = Double.parseDouble(value.getIncome().toString());
		int age = 50;
		int afteryrs = 5;
		if(Integer.parseInt(key.getage())+afteryrs>age)
		{
		if(sal<=500.00){
			pension = sal*Double.parseDouble(pensiondata.get("cat1"));
		}
		else if(sal>=501.00 && sal<=1000.00){
			pension = sal*Double.parseDouble(pensiondata.get("cat2"));
		}
		else if(sal>=1001.00 && sal<=1500.00){
			pension = sal*Double.parseDouble(pensiondata.get("cat3"));
		}
		else if(sal>=1501.00 && sal<=2000.00){
			pension = sal*Double.parseDouble(pensiondata.get("cat4"));
		}
		else if(sal>=1501.00 && sal<=2000.00){
			pension = sal*Double.parseDouble(pensiondata.get("cat5"));
		}
		context.write(new Text("1"), new DoubleWritable(pension));
		}
//	    		String pass = pensiondata.get(key.()); 
//	    		context.write(new Text(pass), value);
		
    
    }  

}
