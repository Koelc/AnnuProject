package projectTeam4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class myMapper4 extends Mapper<myKey, myValue, Text, Text>{
	int cCnt=0;
	int ncCnt=0;
	public void map(myKey k , myValue v, Context c){
		String cob = v.getCob().toString().trim();
		int ww=Integer.parseInt(v.getWW().toString().trim());
		if(cob.equalsIgnoreCase("United-States") && ww>0)
			cCnt++;
		else 
			ncCnt++;	
	}
	public void cleanup(Context c) throws IOException, InterruptedException{
		c.write(new Text("Ratio of employable citizen to immigrants is "), new Text(" "+cCnt +" : " +ncCnt));
	}
}
