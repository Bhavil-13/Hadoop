import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDFStripes {
    
    public static class TFMapper extends Mapper<Object, Text, Text, Text> {
        private Text documentId = new Text();
        private Text termCountPair = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Input format: ID<TAB>TERM<SPACE>COUNT
            // String[] parts = value.toString().split("\t");
            // documentId.set(parts[0]); // Document ID
            // termCountPair.set(parts[1] + "=" + parts[2]); // TERM=COUNT
            // context.write(documentId, termCountPair);
            String[] parts = value.toString().split("\t");
            if (parts.length >= 3) {
                documentId.set(parts[0]); // Document ID
                termCountPair.set(parts[1] + "=" + parts[2]); // TERM=COUNT
                context.write(documentId, termCountPair);  
            }
        }
    }
    
    public static class TFReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> termCountMap = new HashMap<>();
            int totalTerms = 0;
            
            // Count term frequencies for each document
            for (Text value : values) {
                String[] pair = value.toString().split("=");
                String term = pair[0];
                int count = Integer.parseInt(pair[1]);
                termCountMap.put(term, count);
                totalTerms += count;
            }
            
            // Calculate TF-IDF score for each term
            for (Map.Entry<String, Integer> entry : termCountMap.entrySet()) {
                String term = entry.getKey();
                int termFreq = entry.getValue();
                double tfidf = (double) termFreq / totalTerms;
                outputValue.set(term + "\t" + tfidf);
                context.write(key, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TF-IDF Stripes");

        job.setJarByClass(TFIDFStripes.class);
        job.setMapperClass(TFMapper.class);
        job.setReducerClass(TFReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
