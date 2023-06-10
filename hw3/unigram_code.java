import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class unigram_code {

  public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final static Text documentId = new Text();
    private Text term = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String arr[] = line.split("\t", 2);
      documentId.set(arr[0]);
      StringTokenizer itr = new StringTokenizer(arr[1].toLowerCase().replaceAll("[^a-z]+", " "));
      while (itr.hasMoreTokens()) {
        term.set(itr.nextToken());
        context.write(term, documentId);
      }
    }
  }

  public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    private Text docFrequencies = new Text();

    public void reduce(Text term, Iterable<Text> documentIds, Context context)
        throws IOException, InterruptedException {
      HashMap<String, Integer> termOccurrences = new HashMap<String, Integer>();
          
      for (Text documentId : documentIds) {
        if (termOccurrences.containsKey(documentId.toString())) {
          termOccurrences.put(documentId.toString(), termOccurrences.get(documentId.toString()) + 1);
        } else {
          termOccurrences.put(documentId.toString(), 1);
        }
      }

      String frequencyList = "";

      for (String docId : termOccurrences.keySet()) {
        frequencyList += docId + ":" + termOccurrences.get(docId) + " ";
      }
      docFrequencies.set(frequencyList);
      context.write(term, docFrequencies);
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Job job = new Job();
    job.setJarByClass(unigram_code.class);
    job.setJobName("Inverted Index");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(InvertedIndexMapper.class);
    job.setReducerClass(InvertedIndexReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.waitForCompletion(true);
  }
}