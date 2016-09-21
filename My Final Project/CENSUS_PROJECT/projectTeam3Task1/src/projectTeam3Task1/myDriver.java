package projectTeam3Task1;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class myDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
	int choice=0;
	//char c='n';
	Job j= new Job();
	Scanner src = new Scanner(System.in);
//		do{
			System.out.println("Enter your choice: \n 1. Task 1 \n 2. Task 2 \n 3. Task 3");		
				choice = src.nextInt();
		if(choice==1){
			//System.out.println("Enter the number of year after which the pension has to be given");
			//int x = src.nextInt();
			Configuration conf = new Configuration();
			//conf.setInt("Years", x);
			j = new Job(conf,"Team 3 Task 1");
			j.setJarByClass(myDriver.class);
			j.setNumReduceTasks(1);
			j.setInputFormatClass(myInputFormat.class);
			j.setMapperClass(myMapper1.class);
			j.setReducerClass(myReducer1.class);
			j.setMapOutputKeyClass(Text.class);
			j.setMapOutputValueClass(DoubleWritable.class);	
			DistributedCache.addCacheFile(new URI("/user/cloudera/p1/part-m-00000"),j.getConfiguration());

		}
	else if(choice == 2){
		Configuration conf = new Configuration();
			j = new Job(conf,"Team 3 Task 2");
			j.setJarByClass(myDriver.class);
			j.setNumReduceTasks(1);
			j.setInputFormatClass(myInputFormat.class);
			j.setMapperClass(myMapper2.class);
			j.setReducerClass(myReducer2.class);
			j.setMapOutputKeyClass(Text.class);
			j.setMapOutputValueClass(DoubleWritable.class);	
			DistributedCache.addCacheFile(new URI("/user/cloudera/s_category/part-m-00000"),j.getConfiguration());

		}
	else if(choice == 3){
//			System.out.println("Enter the age for which employment is given");
//			int x = src.nextInt();
		Configuration conf = new Configuration();
//			conf.setInt("Age", x);
		j = new Job(conf,"Team 3 Task 1");
		j.setJarByClass(myDriver.class);
		j.setNumReduceTasks(1);
		j.setInputFormatClass(myInputFormat.class);
		j.setMapperClass(myMapper3.class);
	j.setReducerClass(myReducer3.class);
			j.setMapOutputKeyClass(Text.class);
	j.setMapOutputValueClass(IntWritable.class);		
//			DistributedCache.addCacheFile(new URI("/input/someTable3"),j.getConfiguration());

		}
	else{
			System.out.println("You have entered wrong choice");
		}
//	System.out.println("Do you want to continue(y/n)?");
//		c = src.next().charAt(0);
//	}while(c=='y');
	
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));

		System.exit(j.waitForCompletion(true)?0:1);
		
	}

}
