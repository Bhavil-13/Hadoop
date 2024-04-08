import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
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

import opennlp.tools.stemmer.PorterStemmer;

public class DocumentFrequency {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<>();
        private PorterStemmer stemmer = new PorterStemmer();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load stop words from file
            try (BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    stopWords.add(line.trim());
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            Set<String> termsInDoc = new HashSet<>();

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]", "");
                if (!stopWords.contains(token)) { // Skip stop words
                    token = stemmer.stem(token); // Perform stemming
                    termsInDoc.add(token);
                }
            }

            // Emit each term along with document ID (key) and value 1
            for (String term : termsInDoc) {
                word.set(term);
                context.write(word, one);
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
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "document frequency");
        // DistributedCache.addFileToClassPath(new Path("/home/bhavil/Desktop/NoSQL-A2/opennlp-tools-1.9.3.jar"), conf);
        // job.setJarByClass(DocumentFrequency.class);
        // job.addFileToClassPath(new Path("/home/bhavil/Desktop/NoSQL-A2/opennlp-tools-1.9.3.jar"));


        job.setJarByClass(DocumentFrequency.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
