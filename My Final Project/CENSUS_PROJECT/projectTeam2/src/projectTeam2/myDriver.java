package projectTeam2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class myDriver {

	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Scanner src = new Scanner(System.in);
		System.out.println("Enter Choice : \n 1. Task 1 \n 2. Task 2");
		int ch= src.nextInt();
		Job j = new Job();
		if(ch==1){
			Configuration conf = new Configuration();
			j = new Job(conf,"Team 2 Task 1");
			j.setJarByClass(myDriver.class);
			j.setMapperClass(myMapper1.class);
			j.setReducerClass(myReducer1.class);
			j.setNumReduceTasks(1);
			j.setMapOutputKeyClass(Text.class);
			j.setMapOutputValueClass(Text.class);
			j.setInputFormatClass(myInputFormat.class);
			DistributedCache.addCacheFile(new URI("/user/cloudera/tax/part-m-00000"),j.getConfiguration());
		}
		else if(ch==2){
		Configuration conf = new Configuration();
		j = new Job(conf,"Team 2 Task 2");
		j.setJarByClass(myDriver.class);
		j.setMapperClass(myMapper2.class);
		j.setReducerClass(myReducer2.class);
		j.setNumReduceTasks(1);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(Text.class);
		j.setInputFormatClass(myInputFormat.class);
		DistributedCache.addCacheFile(new URI("/user/cloudera/agegroup.dat"),j.getConfiguration());
		}
		else{
			System.out.println("Wrong choice");
		}
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));

		System.exit(j.waitForCompletion(true)?0:1);
	}

}
