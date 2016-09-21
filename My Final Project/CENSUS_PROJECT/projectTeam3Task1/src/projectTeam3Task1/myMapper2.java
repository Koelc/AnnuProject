package projectTeam3Task1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class myMapper2 extends Mapper<myKey, myValue, Text, DoubleWritable>{
private HashMap<String, String> scholarship = new HashMap<String, String>();		
	
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
					String status  = lineParts[0].trim();
					String ss = lineParts[1].trim();
					scholarship.put(status, ss);
					line = br.readLine();
				}
				br.close();
			}
		}
		if (scholarship.isEmpty()) 
		{
			System.out.println("Unable To Load module Data.");
		}
	}
	
	public void map(myKey k,myValue v, Context c) throws IOException, InterruptedException{
		int age=Integer.parseInt(k.getage().toString());
		if(age<=18){
		String parents = v.getParents().toString().trim();
		double schlrshp= Double.parseDouble(scholarship.get(parents));
		c.write(new Text("Scholarship"), new DoubleWritable(schlrshp));
		}
	}
	

}
