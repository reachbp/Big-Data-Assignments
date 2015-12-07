package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final int MISSING = 9999;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		System.out.println("Line after trimming" + line);
		String[] tokens = line.split("\\s+");
		String inlink = tokens[0];
		int num_outlinks = (tokens.length - 2);
		double PR = Double.parseDouble(tokens[tokens.length - 1]);
		String link = "";
		for (int i = 0; i < num_outlinks; i++) {
			System.out.println("tokens - " + tokens[i + 1]);
			link = link + " " + tokens[i + 1];
			context.write(new Text(tokens[i + 1]), new Text(inlink + "#" + PR / num_outlinks));
		}
		link.trim();
		System.out.println("inlink  " + inlink + " outlinks " + link);
		if (link.length() > 0)
			context.write(new Text(inlink), new Text(link));
	}

}