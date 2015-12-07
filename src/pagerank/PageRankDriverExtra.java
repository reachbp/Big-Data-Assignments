package pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.*;
/*This class is responsible for running map reduce job*/
public class PageRankDriverExtra extends Configured implements Tool{
public int run(String[] args) throws Exception
 {
 if(args.length !=2) {
 System.err.println("Usage: Page Rank Driver <input path> <outputpath>");
 System.exit(-1);
 }
 @SuppressWarnings("deprecation")
 int i =1;
 boolean success=false;
 while(i<=3) {
Job job = new Job();
 job.setJarByClass(PageRankDriverExtra.class);
 job.setJobName("Page Rank");
 FileInputFormat.addInputPath(job, new Path("/user/bharathi/hw3/"+args[0]));
 FileOutputFormat.setOutputPath(job,new Path("/user/bharathi/hw3/"+args[1]));
 FileSystem fs = FileSystem.get(new Configuration());
 fs.delete(new Path("/user/bharathi/hw3/"+args[1]), true);
 job.setMapperClass(PageRankMapper.class);
 job.setReducerClass(PageRankReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(Text.class);
 success = job.waitForCompletion(true);
 //System.out.println("/user/bharathi/hw3/"+args[0]+  "  "+"/user/bharathi/hw3/"+args[1]);
 if(success) {
	   args[0]="output"+i+"/part-r-00000"; 
	   args[1]="output"+(i+1)+"";i++;
	   fs.delete(new Path("/user/bharathi/hw3/"+args[1]), true);
 }
 }
 return success ? 0 : 1;
 }
public static void main(String[] args) throws Exception {
	PageRankDriverExtra driver = new PageRankDriverExtra();
 int exitCode = ToolRunner.run(driver, args);
 System.exit(exitCode);
 }
}
