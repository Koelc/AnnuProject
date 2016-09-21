package projectTeam2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer2 extends Reducer<Text,Text,Text,Text> {
	int pop=0,fpop=0,mpop=0,apop=0,midpop=0,spop=0,epop=0;
	double fincome=0.0,mincome=0.0,aincome=0.0,midincome=0.0,sincome=0.0,eincome=0.0,totincome=0.0;	
	double pci=0.0,fpci=0.0,mpci=0.0,apci=0.0,midpci=0.0,spci=0.0,epci=0.0;
	public void reduce(Text k , Iterable<Text> v, Context c) throws IOException, InterruptedException{
		for(Text each: v){
			String[] eachVal=each.toString().split(",");
			pop++;
			if(eachVal[0].equalsIgnoreCase("Female")){
				fincome+=Double.parseDouble(eachVal[1]);
				fpop++;
			}
			if(eachVal[0].equalsIgnoreCase("male")){
				mincome+=Double.parseDouble(eachVal[1]);
				mpop++;
			}
			if(eachVal[2].equalsIgnoreCase("adult")){
				aincome+=Double.parseDouble(eachVal[1]);
				apop++;
			}
			if(eachVal[2].equalsIgnoreCase("middle-aged")){
				midincome+=Double.parseDouble(eachVal[1]);
				midpop++;
			}
			if(eachVal[2].equalsIgnoreCase("senior citizen")){
				sincome+=Double.parseDouble(eachVal[1]);
				spop++;
			}
			if(eachVal[2].equalsIgnoreCase("elderly")){
				eincome+=Double.parseDouble(eachVal[1]);
				epop++;
			}
			
		}
		totincome=fincome+mincome;
		pci=totincome/pop;
		fpci=fincome/fpop;
		mpci=mincome/mpop;
		apci=aincome/apop;
		midpci=midincome/midpop;
		spci=sincome/spop;
		epci=eincome/epop;
		String PCI = "PCI : "+pci +"\n";
		String fPCI = "PCI (Female) : "+fpci+"\n";
		String mPCI = "PCI (Male) : "+mpci+"\n";
		String aPCI = "PCI (Adult) : "+apci+"\n";
		String midPCI = "PCI (Middle-Age) : "+midpci+"\n";
		String sPCI = "PCI (Senior Citizen) : "+spci+"\n";
		String ePCI = "PCI (Elderly) : "+epci+"\n";
		c.write(new Text(PCI+fPCI+mPCI+aPCI), new Text(midPCI+sPCI+ePCI));
		
	}

}
