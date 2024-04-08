// import java.io.IOException;
// import java.util.ArrayList;
// import java.util.List;
// import java.util.StringTokenizer;

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// public class CoOccurrenceMatrix {
    
//     public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
//         private final static IntWritable one = new IntWritable(1);
//         private Text word = new Text();

//         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//             StringTokenizer itr = new StringTokenizer(value.toString());
//             List<String> words = new ArrayList<>();
//             while (itr.hasMoreTokens()) {
//                 words.add(itr.nextToken());
//             }
//             int d = context.getConfiguration().getInt("word.distance", 1);
//             for (int i = 0; i < words.size(); i++) {
//                 word.set(words.get(i));
//                 int start = Math.max(0, i - d);
//                 int end = Math.min(words.size(), i + d + 1);
//                 for (int j = start; j < end; j++) {
//                     if (j != i) {
//                         Text coWord = new Text(words.get(j));
//                         context.write(new Text(word.toString() + "," + coWord.toString()), one);
//                     }
//                 }
//             }
//         }
//     }

//     public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//         private IntWritable result = new IntWritable();

//         public void reduce(Text key, Iterable<IntWritable> values, Context context)
//                 throws IOException, InterruptedException {
//             int sum = 0;
//             for (IntWritable val : values) {
//                 sum += val.get();
//             }
//             result.set(sum);
//             context.write(key, result);
//         }
//     }

//     public static void main(String[] args) throws Exception {
//         int[] distances = {1, 2, 3, 4}; // Word distances to consider
//         for (int d : distances) {
//             Configuration conf = new Configuration();
//             conf.setInt("word.distance", d);

//             Job job = Job.getInstance(conf, "co-occurrence-matrix-d-" + d);
//             job.setJarByClass(CoOccurrenceMatrix.class);
//             job.setMapperClass(TokenizerMapper.class);
//             job.setCombinerClass(IntSumReducer.class); // Optional
//             job.setReducerClass(IntSumReducer.class);

//             job.setOutputKeyClass(Text.class);
//             job.setOutputValueClass(IntWritable.class);

//             FileInputFormat.addInputPath(job, new Path(args[0]));
//             FileOutputFormat.setOutputPath(job, new Path(args[1] + "-d-" + d));

//             job.waitForCompletion(true);
//         }
//     }
// }

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CoOccurrenceMatrix {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            List<String> words = new ArrayList<>();
            while (itr.hasMoreTokens()) {
                words.add(itr.nextToken());
            }
            int d = context.getConfiguration().getInt("word.distance", 1);
            for (int i = 0; i < words.size(); i++) {
                word.set(words.get(i));
                int start = Math.max(0, i - d);
                int end = Math.min(words.size(), i + d + 1);
                for (int j = start; j < end; j++) {
                    if (j != i) {
                        Text coWord = new Text(words.get(j));
                        context.write(new Text(word.toString() + "," + coWord.toString()), one);
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        int[] distances = {1, 2, 3, 4}; // Word distances to consider
        for (int d : distances) {
            Configuration conf = new Configuration();
            conf.setInt("word.distance", d);

            Job job = Job.getInstance(conf, "co-occurrence-matrix-d-" + d);
            job.setJarByClass(CoOccurrenceMatrix.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "-d-" + d));

            long startTime = System.currentTimeMillis();
            job.waitForCompletion(true);
            long endTime = System.currentTimeMillis();

            System.out.println("Runtime for d = " + d + ": " + (endTime - startTime) + " milliseconds");
        }
    }
}

// This Java code implements a MapReduce program using Apache Hadoop to construct a co-occurrence matrix from a collection of documents. The program consists of two main classes: TokenizerMapper and IntSumReducer. The TokenizerMapper tokenizes input text into words and emits pairs of words along with a count of 1, where the pairs represent co-occurring words within a specified distance. The IntSumReducer class aggregates counts for each pair of words by summing up the values. The main method configures the MapReduce job with different word distances, runs the job for each distance, and records the runtime. Overall, this program efficiently processes large volumes of text data to construct a co-occurrence matrix, which can be used for various natural language processing tasks like semantic analysis and information retrieval.

// import java.io.IOException;
// import java.util.StringTokenizer;

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// public class CoOccurrenceMatrixPairs {

//     public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
//         private final static IntWritable one = new IntWritable(1);
//         private Text wordPair = new Text();

//         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//             StringTokenizer itr = new StringTokenizer(value.toString());
//             while (itr.hasMoreTokens()) {
//                 String token = itr.nextToken().toLowerCase();
//                 String[] words = token.split("\\s+");
//                 if (words.length > 1) {
//                     for (int i = 0; i < words.length; i++) {
//                         for (int j = i + 1; j < words.length; j++) {
//                             wordPair.set(words[i] + "," + words[j]);
//                             context.write(wordPair, one);
//                             wordPair.set(words[j] + "," + words[i]);
//                             context.write(wordPair, one); // Include reverse order as well
//                         }
//                     }
//                 }
//             }
//         }
//     }

//     public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//         private IntWritable result = new IntWritable();

//         public void reduce(Text key, Iterable<IntWritable> values, Context context)
//                 throws IOException, InterruptedException {
//             int sum = 0;
//             for (IntWritable val : values) {
//                 sum += val.get();
//             }
//             result.set(sum);
//             context.write(key, result);
//         }
//     }

//     public static void main(String[] args) throws Exception {
//         Configuration conf = new Configuration();
//         Job job = Job.getInstance(conf, "co-occurrence-matrix-pairs");

//         job.setJarByClass(CoOccurrenceMatrixPairs.class);
//         job.setMapperClass(TokenizerMapper.class);
//         job.setCombinerClass(IntSumReducer.class); // Optional
//         job.setReducerClass(IntSumReducer.class);

//         job.setOutputKeyClass(Text.class);
//         job.setOutputValueClass(IntWritable.class);

//         FileInputFormat.addInputPath(job, new Path(args[0]));
//         FileOutputFormat.setOutputPath(job, new Path(args[1]));

//         System.exit(job.waitForCompletion(true) ? 0 : 1);
//     }
// }
