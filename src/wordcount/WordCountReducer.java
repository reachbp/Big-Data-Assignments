package wordcount;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class WordCountReducer
extends Reducer<Text, IntWritable, Text, IntWritable> {
@Override
public void reduce(Text key, Iterable<IntWritable> values,
 Context context)
 throws IOException, InterruptedException {
 int wordCount=0;
 //.out.println("Key "+key);
 for (IntWritable value : values) {
//	 System.out.print("Values "+value);
wordCount+= value.get();
 }
 //System.out.println();
 context.write(key, new IntWritable(wordCount));
}
}