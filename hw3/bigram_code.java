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

public class bigram_code {

  public static class InvertedIndexBigramMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final static Text documentId = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String arr[] = line.split("\t", 2);
      documentId.set(arr[0]);
      String[] terms = arr[1].toLowerCase().replaceAll("[^a-z]+", " ").split(" ");

      for (int i = 0; i < terms.length - 1; i++) {
        String currentWord = terms[i];
        String nextWord = terms[i + 1];
        String bigram = currentWord + " " + nextWord;

        if (bigram.equals("computer science") ||
            bigram.equals("information retrieval") ||
            bigram.equals("power politics") ||
            bigram.equals("los angeles") ||
            bigram.equals("bruce willis")) {
          context.write(new Text(bigram), documentId);
        }
      }
    }

  }

  public static class InvertedIndexBigramReducer extends Reducer<Text, Text, Text, Text> {
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
    job.setJarByClass(bigram_code.class);
    job.setJobName("Inverted Index Bigram");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(InvertedIndexBigramMapper.class);
    job.setReducerClass(InvertedIndexBigramReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.waitForCompletion(true);
  }
}