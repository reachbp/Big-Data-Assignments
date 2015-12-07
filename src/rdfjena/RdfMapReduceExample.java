package rdfjena;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.jena.hadoop.rdf.io.input.TriplesInputFormat;
import org.apache.jena.hadoop.rdf.io.output.ntriples.NTriplesNodeOutputFormat;
import org.apache.jena.hadoop.rdf.mapreduce.count.NodeCountReducer;
import org.apache.jena.hadoop.rdf.mapreduce.count.TripleNodeCountMapper;
import org.apache.jena.hadoop.rdf.types.NodeWritable;

public class RdfMapReduceExample {

    public static void main(String[] args) {
        try {
            // Get Hadoop configuration
            Configuration config = new Configuration(true);

            // Create job
            Job job = Job.getInstance(config);
            job.setJarByClass(RdfMapReduceExample.class);
            job.setJobName("RDF Triples Node Usage Count");

            // Map/Reduce classes
            job.setMapperClass(TripleNodeCountMapper.class);
            job.setMapOutputKeyClass(NodeWritable.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setReducerClass(NodeCountReducer.class);

            // Input and Output
            job.setInputFormatClass(TriplesInputFormat.class);
            job.setOutputFormatClass(NTriplesNodeOutputFormat.class);
            FileInputFormat.setInputPaths(job, new Path("/example/input/"));
            FileOutputFormat.setOutputPath(job, new Path("/example/output/"));

            // Launch the job and await completion
            job.submit();
            if (job.monitorAndPrintJob()) {
                // OK
                System.out.println("Completed");
            } else {
                // Failed
                System.err.println("Failed");
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
