import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class WordCountMapper extends
 Mapper<LongWritable, Text, Text, IntWritable> {
private static final int MISSING = 9999;
@Override
 public void map(LongWritable key, Text value, Context context)
 throws IOException, InterruptedException {
String line = value.toString();
String[] tokens = line.split(",");
String date=tokens[0];
String time=tokens[1];String name=tokens[2];
String tweet=tokens[3];
String[] searchWords=new String[]{"hackathon", "Dec", "Chicago", "Java"};
 //Search each line for any of the words
for(int i =0;i<4;i++){
	 if (line.contains(searchWords[i])) {
		//System.out.println("Year "+year+ " air Temp"+airTemperature);
		 context.write(new Text(searchWords[i]), new IntWritable(1));
		 }
	 else
		 context.write(new Text(searchWords[i]), new IntWritable(0));
}
}
}