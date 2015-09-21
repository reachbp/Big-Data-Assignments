package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class PageRankReducer
extends Reducer<Text, Text, Text, Text> {
@Override
public void reduce(Text key, Iterable<Text> values,
 Context context)
 throws IOException, InterruptedException {
 Double PR_total=0.0;
 System.out.println("Key "+key);
 String outLink=key.toString();
 for (Text value : values) {
	 System.out.println("value is " +value.toString());
	String[] tokens=value.toString().split("#");
	if(tokens.length==2) //rank value pair
	{
		PR_total+=Double.parseDouble(tokens[1]);
	}
	else {	//split by space
		String outlinks=value.toString().trim();
		outLink=outLink+" "+outlinks.toString();
	}
 
 }
System.out.println("outlink is " +outLink);
 context.write(new Text(outLink.trim()), new Text(PR_total.toString()));
}
}