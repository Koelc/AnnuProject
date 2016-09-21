package projectTeam2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class myMapper2 extends Mapper<myKey, myValue, Text, Text>{
	HashMap<String, String> ageGrp = new HashMap<String, String>();
	public void setup(Context c) throws IOException{
		Path[] allFiles = DistributedCache.getLocalCacheFiles(c.getConfiguration());		
		for(Path eachFile : allFiles){
			if(eachFile.getName().equals("agegroup.dat")){
				FileReader fr = new FileReader(eachFile.toString());
				BufferedReader br = new BufferedReader(fr);
				String line =br.readLine();
				while(line != null){
					String[] eachVal = line.split("\t");
					String age = eachVal[0];
					String grp = eachVal[1].trim();
					ageGrp.put(age, grp);
					line =br.readLine();
				}
				br.close();
			}
			if (ageGrp.isEmpty()) 
			{
				throw new IOException("Unable To Load Customer Data.");
			}
		}
		
	}
	public void map(myKey k , myValue v ,Context c) throws IOException, InterruptedException{
		double income=Double.parseDouble(v.getIncome().toString());
		String gender=v.getGender().toString().trim();
		String age=k.getage().toString();
		String cat =ageGrp.get(age);
		c.write(new Text("Unique"), new Text(gender+","+income+","+cat));
	}

}
