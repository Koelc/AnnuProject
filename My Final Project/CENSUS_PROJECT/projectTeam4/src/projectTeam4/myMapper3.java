package projectTeam4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class myMapper3 extends Mapper<myKey, myValue, Text, Text>{
	int fcnt=0;
	int mcnt=0;
	public void map(myKey k , myValue v, Context c){
		String gen = v.getGender().toString().trim();
		if(gen.equalsIgnoreCase("female"))
			fcnt++;
		else 
			mcnt++;	
	}
	
public void cleanup(Context c) throws IOException, InterruptedException{
		//int total = fcnt+mcnt;
		//double f = ((fcnt/total)*100);
		//double m =((mcnt/total)*100);
		c.write(new Text("Sex ratio is : "), new Text(" "+fcnt +" : " +mcnt));
	}

}
