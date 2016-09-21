package projectTeam2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class myMapper1 extends Mapper<myKey, myValue, Text, Object>{
	
	
	HashMap<secKey, secVal> taxSec = new HashMap<secKey, secVal>();
	public void setup(Context c) throws IOException{
		Path[] files = DistributedCache.getLocalCacheFiles(c.getConfiguration());		
		for (Path SinglePath : files) {
			if (SinglePath.getName().equals("part-m-00000")) 
			{
	BufferedReader br = new BufferedReader(new FileReader(SinglePath.toString()));
				String line = br.readLine();
				while(line != null) 
				{
					String[] lineParts = line.split(",");
					
					double min  = Double.parseDouble(lineParts[0]);
					double max  = Double.parseDouble(lineParts[1]);
					String gen = lineParts[2];
					int tax = Integer.parseInt(lineParts[3]);
					secKey k1 = new secKey(min,max,gen);
					secVal v1 = new secVal(tax);
					taxSec.put(k1, v1);
					line = br.readLine();
				}
				br.close();
			}
		}
		if (taxSec.isEmpty()) 
		{
			System.out.println("Unable To Load module Data.");
		}
	}
	
	public void map(myKey k, myValue v, Context c) throws IOException, InterruptedException{
		double taxamt =0.0;
		int per=0;
		double income = Double.parseDouble(v.getIncome().toString());
		String gender = v.getGender().toString().trim();
		Set<secKey> keyP = taxSec.keySet();
		for(secKey g: keyP ){
			//per =taxSec.get(g).getTax();
			double min = g.getMin();
			double max = g.getMax();
			String gen = g.getGen().trim();
			if((gender.equalsIgnoreCase(gen)) && (income>=min && income<=max)){
				
				per =taxSec.get(g).getTax();
			}
		}
		taxamt=per*income/100;
		c.write(new Text("Unique"), new Text(taxamt+","+income));
		
	}

}
