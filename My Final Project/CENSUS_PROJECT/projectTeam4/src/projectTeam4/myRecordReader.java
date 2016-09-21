package projectTeam4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class myRecordReader extends RecordReader<myKey, myValue> {
	myKey key;
	myValue val;
	LineRecordReader reader = new LineRecordReader();

	@Override
	public void close() throws IOException {
		reader.close();

	}

	@Override
	public myKey getCurrentKey() throws IOException, InterruptedException {
		
		return key;
	}

	@Override
	public myValue getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return val;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		reader.initialize(arg0, arg1);

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean getnextVal = reader.nextKeyValue();
		if(getnextVal){
			if(key == null)
				key = new myKey();
			if(val == null)
				val = new myValue();
			Text line = reader.getCurrentValue();
			String[] each = line.toString().split(",");
			key.setage(new Text(each[0]));
			val.setEducation(new Text(each[1]));
			val.setTaxfiler(new Text(each[4]));
			val.setGender(new Text(each[3]));
			val.setCitiznshp(new Text(each[8]));
			val.setIncome(new Text(each[5]));
			val.setMStatus(new Text(each[2]));
			val.setParents(new Text(each[6]));
			val.setCob(new Text(each[7]));
			val.setWW(new Text(each[9]));	
	
		}
		else{
			key = null;
			val = null;	
		}
		return getnextVal;
	}

}
