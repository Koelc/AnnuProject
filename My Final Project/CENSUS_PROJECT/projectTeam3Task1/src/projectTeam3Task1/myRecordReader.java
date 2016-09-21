package projectTeam3Task1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


public class myRecordReader extends RecordReader<myKey, myValue> {
	myKey key;
	myValue value;
	private LineRecordReader reader = new LineRecordReader();
	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public myKey getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public myValue getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		reader.initialize(arg0, arg1);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		boolean nextValue = reader.nextKeyValue();
		if(nextValue){
		if(key==null){
			key = new myKey();
		}
		if(value == null){
			value = new myValue();
		}
		Text line = reader.getCurrentValue();
		String[] tokens = line.toString().split(",");
		key.setage(new Text(tokens[0]));
		value.setIncome(new Text(tokens[5]));
		value.setMStatus(new Text(tokens[2]));
		value.setGender(new Text(tokens[3]));
		value.setParents(new Text(tokens[6]));
	}
		else{
			key = null;
			value = null;	
		}
		return nextValue;
	}

}
