// import java.io.IOException;
// import java.util.HashMap;
// import java.util.Map;
// import java.util.StringTokenizer;

// import org.apache.hadoop.io.Writable;

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.MapWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// public class CoOccurrenceStripe {

//     public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {
//         private final static MapWritable map = new MapWritable();
//         private Text word = new Text();

//         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//             StringTokenizer itr = new StringTokenizer(value.toString());
//             String[] words = new String[itr.countTokens()];
//             int i = 0;
//             while (itr.hasMoreTokens()) {
//                 words[i++] = itr.nextToken();
//             }
//             int d = context.getConfiguration().getInt("word.distance", 1);
//             for (i = 0; i < words.length; i++) {
//                 map.clear();
//                 word.set(words[i]);
//                 int start = Math.max(0, i - d);
//                 int end = Math.min(words.length, i + d + 1);
//                 for (int j = start; j < end; j++) {
//                     if (j != i) {
//                         Text coWord = new Text(words[j]);
//                         if (map.containsKey(coWord)) {
//                             map.put(coWord, new Text((Integer.parseInt(map.get(coWord).toString()) + 1) + ""));
//                         } else {
//                             map.put(coWord, new Text("1"));
//                         }
//                     }
//                 }
//                 context.write(word, map);
//             }
//         }
//     }

//     public static class IntSumReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
//         private MapWritable result = new MapWritable();

//         public void reduce(Text key, Iterable<MapWritable> values, Context context)
//                 throws IOException, InterruptedException {
//             result.clear();
//             for (MapWritable val : values) {
//                 addMaps(val);
//             }
//             context.write(key, result);
//         }

//         private void addMaps(MapWritable map) {
//             for (Map.Entry<Writable, Writable> entry : map.entrySet()) {
//                 Text word = (Text) entry.getKey();
//                 Text count = (Text) entry.getValue();
//                 if (result.containsKey(word)) {
//                     int sum = Integer.parseInt(result.get(word).toString()) + Integer.parseInt(count.toString());
//                     result.put(word, new Text(sum + ""));
//                 } else {
//                     result.put(word, count);
//                 }
//             }
//         }
//     }

//     public static void main(String[] args) throws Exception {
//         int[] distances = {1, 2, 3, 4}; // Word distances to consider
//         for (int d : distances) {
//             Configuration conf = new Configuration();
//             conf.setInt("word.distance", d);

//             Job job = Job.getInstance(conf, "co-occurrence-stripe-d-" + d);
//             job.setJarByClass(CoOccurrenceStripe.class);
//             job.setMapperClass(TokenizerMapper.class);
//             job.setCombinerClass(IntSumReducer.class); // Optional
//             job.setReducerClass(IntSumReducer.class);

//             job.setOutputKeyClass(Text.class);
//             job.setOutputValueClass(MapWritable.class);

//             FileInputFormat.addInputPath(job, new Path(args[0]));
//             FileOutputFormat.setOutputPath(job, new Path(args[1] + "-d-" + d));

//             job.waitForCompletion(true);
//         }
//     }
// }


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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

public class CoOccurrenceStripe {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] words = new String[itr.countTokens()];
            int i = 0;
            while (itr.hasMoreTokens()) {
                words[i++] = itr.nextToken();
            }
            int d = context.getConfiguration().getInt("word.distance", 1);
            for (i = 0; i < words.length; i++) {
                Map<String, Integer> stripe = new HashMap<>();
                int start = Math.max(0, i - d);
                int end = Math.min(words.length, i + d + 1);
                for (int j = start; j < end; j++) {
                    if (j != i) {
                        String coWord = words[j];
                        stripe.put(coWord, stripe.getOrDefault(coWord, 0) + 1);
                    }
                }
                word.set(words[i]);
                context.write(word, new Text(stripe.toString()));
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> stripe = new HashMap<>();
            for (Text val : values) {
                mergeStripe(stripe, val.toString());
            }
            result.set(stripe.toString());
            context.write(key, result);
        }


        private void mergeStripe(Map<String, Integer> target, String stripe) {
            stripe = stripe.substring(1, stripe.length() - 1); // Removing '{' and '}' characters
            String[] pairs = stripe.split(",");
            for (String pair : pairs) {
                String[] keyValue = pair.trim().split("=");
                if (keyValue.length == 2) { // Check if keyValue has at least two elements
                    String word = keyValue[0].trim();
                    int count = Integer.parseInt(keyValue[1].trim());
                    target.put(word, target.getOrDefault(word, 0) + count);
                }
            }
        }

        
        // private void mergeStripe(Map<String, Integer> target, String stripe) {
        //     stripe = stripe.substring(1, stripe.length() - 1); // Removing '{' and '}' characters
        //     String[] pairs = stripe.split(",");
        //     for (String pair : pairs) {
        //         String[] keyValue = pair.trim().split("=");
        //         String word = keyValue[0].trim();
        //         int count = Integer.parseInt(keyValue[1].trim());
        //         target.put(word, target.getOrDefault(word, 0) + count);
        //     }
        // }
    }

    public static void main(String[] args) throws Exception {
        int[] distances = {1, 2, 3, 4}; // Word distances to consider
        for (int d : distances) {
            Configuration conf = new Configuration();
            conf.setInt("word.distance", d);

            Job job = Job.getInstance(conf, "co-occurrence-stripe-d-" + d);
            job.setJarByClass(CoOccurrenceStripe.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class); // Optional
            job.setReducerClass(IntSumReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "-d-" + d));

            long startTime = System.currentTimeMillis();
            job.waitForCompletion(true);
            long endTime = System.currentTimeMillis();

            System.out.println("Runtime for d = " + d + ": " + (endTime - startTime) + " milliseconds");
        }
    }
}