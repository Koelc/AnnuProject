package projectTeam4;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class myDeriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Scanner src = new Scanner(System.in);
		System.out.println("Enter Choice : \n 1. Task 1 \n 2. Task 2 \n 3. Task 3 \n 4. Task 4");
		int ch= src.nextInt();
		Job j = new Job();
		if(ch==1){
		System.out.println("Enter the no of years after which the election will occur");
		int yrs = src.nextInt();
		Configuration conf = new Configuration();
		conf.setInt("years", yrs);
		j = new Job(conf,"Task 1");
		j.setJarByClass(myDeriver.class);
		j.setMapperClass(myMapper1.class);
		j.setReducerClass(myReducer1.class);
		j.setNumReduceTasks(1);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(IntWritable.class);
		j.setInputFormatClass(myInputFormat.class);
		}
		else if(ch==2){
			System.out.println("Enter the no of years after which the calculation has to be done");
			int yrs = src.nextInt();
			System.out.println("Enter the no of age of senior citizen");
			int age = src.nextInt();
			Configuration conf = new Configuration();
			conf.setInt("years", yrs);
			conf.setInt("age", age);
			j = new Job(conf,"Task 2");
			j.setJarByClass(myDeriver.class);
			j.setMapperClass(myMapper2.class);
			j.setReducerClass(myReducer1.class);
			j.setNumReduceTasks(1);
			j.setMapOutputKeyClass(Text.class);
			j.setMapOutputValueClass(IntWritable.class);
			j.setInputFormatClass(myInputFormat.class);
		}
		else if(ch==3){
			Configuration conf = new Configuration();
			j = new Job(conf,"Task 3");
			j.setJarByClass(myDeriver.class);
			j.setMapperClass(myMapper3.class);
			j.setNumReduceTasks(0);
			j.setMapOutputKeyClass(Text.class);
			j.setMapOutputValueClass(Text.class);
			j.setInputFormatClass(myInputFormat.class);
		}
		else if(ch==4){
			Configuration conf = new Configuration();
			j = new Job(conf,"Task 4");
			j.setJarByClass(myDeriver.class);
			j.setMapperClass(myMapper4.class);
			j.setNumReduceTasks(0);
			j.setMapOutputKeyClass(Text.class);
			j.setMapOutputValueClass(Text.class);
			j.setInputFormatClass(myInputFormat.class);
		}
		else{
			System.out.println("Wrong choice");
		}
		
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path (args[1]));
		
		System.exit(j.waitForCompletion(true)?0:1);

	}

}
