// import java.io.BufferedReader;
// import java.io.FileReader;
// import java.io.IOException;
// import java.util.HashMap;
// import java.util.Map;
// import java.util.HashSet;
// import java.net.URI;
// import java.io.File;


// import java.util.StringTokenizer;
// import java.util.List;
// import java.util.ArrayList;



// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// public class TopWords {
    
//     // Mapper class
//     public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        
//         private final static IntWritable one = new IntWritable(1);
//         private Text word = new Text();
//         private HashSet<String> stopWords = new HashSet<>();
        
//         // Load stop words from the distributed cache
//         protected void setup(Context context) throws IOException, InterruptedException {
//             URI[] stopWordFiles = context.getCacheFiles();
//             if (stopWordFiles != null && stopWordFiles.length > 0) {
//                 for (URI stopWordFile : stopWordFiles) {
//                     loadStopWords(stopWordFile, context);
//                 }
//             }
//         }

//         // Load stop words into HashSet
//         private void loadStopWords(URI stopWordFile, Context context) throws IOException {
//             try {
//                 BufferedReader reader = new BufferedReader(new FileReader(new File(stopWordFile)));
//                 String line;
//                 while ((line = reader.readLine()) != null) {
//                     stopWords.add(line.trim());
//                 }
//                 reader.close();
//             } catch (Exception e) {
//                 System.err.println("Error reading stop words file: " + e.getMessage());
//             }
//         }
        
//         public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//             String line = value.toString();
//             // Tokenize the line
//             String[] words = line.split("\\s+");

//             // Process each word and emit key-value pairs
//             for (String word : words) {
//                 // Convert to lowercase and remove punctuation
//                 word = word.toLowerCase().replaceAll("[^a-zA-Z]", "");
//                 if (!word.isEmpty() && !stopWords.contains(word)) {
//                     this.word.set(word);
//                     context.write(this.word, one);
//                 }
//             }
//         }
//     }
    
//     // Reducer class
//     public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        
//         private Map<Text, IntWritable> countMap = new HashMap<>();
        
//         public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//             // Sum up the counts for each word
//             int sum = 0;
//             for (IntWritable val : values) {
//                 sum += val.get();
//             }
//             // Store the word and its count in the map
//             countMap.put(new Text(key), new IntWritable(sum));
//         }
        
//         protected void cleanup(Context context) throws IOException, InterruptedException {
//             // Sort the map by value and emit the top 50 words
//             countMap.entrySet().stream()
//                 .sorted(Map.Entry.<Text, IntWritable>comparingByValue().reversed())
//                 .limit(50)
//                 .forEach(entry -> {
//                     try {
//                         context.write(entry.getKey(), entry.getValue());
//                     } catch (IOException | InterruptedException e) {
//                         e.printStackTrace();
//                     }
//                 });
//         }
//     }

//     public static void main(String[] args) throws Exception {
//         Configuration conf = new Configuration();
//         Job job = Job.getInstance(conf, "top_words");


//         job.setJarByClass(TopWords.class);
//         job.setMapperClass(TokenizerMapper.class);
//         job.setReducerClass(IntSumReducer.class);

//         job.setOutputKeyClass(Text.class);
//         job.setOutputValueClass(IntWritable.class);

//         // Input and output format
//         job.setInputFormatClass(TextInputFormat.class);
//         job.setOutputFormatClass(TextOutputFormat.class);

//         FileInputFormat.addInputPath(job, new Path(args[0]));
//         FileOutputFormat.setOutputPath(job, new Path(args[1]));

// 		job.waitForCompletion(true);
//     }
// }

// =================================================================================================================================================

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

// ==============================================================================================================================

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


// ==================================================================================================================================================

// public class CoOccurrenceStripe {

//     public static class MapAggregationMapper extends Mapper<Object, Text, Text, MapWritable> {
//         private final Map<String, MapWritable> partialResults = new HashMap<>();

//         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//             StringTokenizer itr = new StringTokenizer(value.toString());
//             String[] words = new String[itr.countTokens()];
//             int i = 0;
//             while (itr.hasMoreTokens()) {
//                 words[i++] = itr.nextToken();
//             }
//             int d = context.getConfiguration().getInt("word.distance", 1);
//             for (i = 0; i < words.length; i++) {
//                 MapWritable stripe = partialResults.computeIfAbsent(words[i], k -> new MapWritable());
//                 int start = Math.max(0, i - d);
//                 int end = Math.min(words.length, i + d + 1);
//                 for (int j = start; j < end; j++) {
//                     if (j != i) {
//                         Text coWord = new Text(words[j]);
//                         if (stripe.containsKey(coWord)) {
//                             stripe.put(coWord, new Text((Integer.parseInt(stripe.get(coWord).toString()) + 1) + ""));
//                         } else {
//                             stripe.put(coWord, new Text("1"));
//                         }
//                     }
//                 }
//             }
//         }

//         @Override
//         protected void cleanup(Context context) throws IOException, InterruptedException {
//             for (Map.Entry<String, MapWritable> entry : partialResults.entrySet()) {
//                 context.write(new Text(entry.getKey()), entry.getValue());
//             }
//         }
//     }

//     public static class MapFunctionMapper extends Mapper<Object, Text, Text, MapWritable> {
//         private final MapWritable map = new MapWritable();
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
//             Configuration conf1 = new Configuration();
//             conf1.setInt("word.distance", d);

//             Job job1 = Job.getInstance(conf1, "map-aggregation-stripe-d-" + d);
//             job1.setJarByClass(CoOccurrenceStripe.class);
//             job1.setMapperClass(MapAggregationMapper.class);
//             job1.setReducerClass(IntSumReducer.class);

//             job1.setOutputKeyClass(Text.class);
//             job1.setOutputValueClass(MapWritable.class);

//             FileInputFormat.addInputPath(job1, new Path(args[0]));
//             FileOutputFormat.setOutputPath(job1, new Path(args[1] + "-map-aggregation-d-" + d));

//             job1.waitForCompletion(true);

//             Configuration conf2 = new Configuration();
//             conf2.setInt("word.distance", d);

//             Job job2 = Job.getInstance(conf2, "map-function-stripe-d-" + d);
//             job2.setJarByClass(CoOccurrenceStripe.class);
//             job2.setMapperClass(MapFunctionMapper.class);
//             job2.setReducerClass(IntSumReducer.class);

//             job2.setOutputKeyClass(Text.class);
//             job2.setOutputValueClass(MapWritable.class);

//             FileInputFormat.addInputPath(job2, new Path(args[0]));
//             FileOutputFormat.setOutputPath(job2, new Path(args[1] + "-map-function-d-" + d));

//             job2.waitForCompletion(true);
//         }
//     }
// }

// ==========================================================================================================================

// public class DocumentFrequency {

//     public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
//         private final static IntWritable one = new IntWritable(1);
//         private Text word = new Text();
//         private Set<String> stopWords = new HashSet<>();
//         private PorterStemmer stemmer = new PorterStemmer();

//         @Override
//         protected void setup(Context context) throws IOException, InterruptedException {
//             // Load stop words from file
//             try (BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"))) {
//                 String line;
//                 while ((line = br.readLine()) != null) {
//                     stopWords.add(line.trim());
//                 }
//             }
//         }

//         @Override
//         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//             StringTokenizer itr = new StringTokenizer(value.toString());
//             Set<String> termsInDoc = new HashSet<>();

//             while (itr.hasMoreTokens()) {
//                 String token = itr.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]", "");
//                 if (!stopWords.contains(token)) { // Skip stop words
//                     token = stemmer.stem(token); // Perform stemming
//                     termsInDoc.add(token);
//                 }
//             }

//             // Emit each term along with document ID (key) and value 1
//             for (String term : termsInDoc) {
//                 word.set(term);
//                 context.write(word, one);
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
//         Job job = Job.getInstance(conf, "document frequency");

//         job.setJarByClass(DocumentFrequency.class);
//         job.setMapperClass(TokenizerMapper.class);
//         job.setCombinerClass(IntSumReducer.class);
//         job.setReducerClass(IntSumReducer.class);

//         job.setOutputKeyClass(Text.class);
//         job.setOutputValueClass(IntWritable.class);

//         FileInputFormat.addInputPath(job, new Path(args[0]));
//         FileOutputFormat.setOutputPath(job, new Path(args[1]));

//         System.exit(job.waitForCompletion(true) ? 0 : 1);
//     }
// }

// =============================================================================================================================================


// public class TFIDFStripes {
    
//     public static class TFMapper extends Mapper<Object, Text, Text, Text> {
//         private Text documentId = new Text();
//         private Text termCountPair = new Text();

//         @Override
//         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//             // Input format: ID<TAB>TERM<SPACE>COUNT
//             String[] parts = value.toString().split("\t");
//             documentId.set(parts[0]); // Document ID
//             termCountPair.set(parts[1] + "=" + parts[2]); // TERM=COUNT
//             context.write(documentId, termCountPair);
//         }
//     }
    
//     public static class TFReducer extends Reducer<Text, Text, Text, Text> {
//         private Text outputValue = new Text();

//         @Override
//         public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//             Map<String, Integer> termCountMap = new HashMap<>();
//             int totalTerms = 0;
            
//             // Count term frequencies for each document
//             for (Text value : values) {
//                 String[] pair = value.toString().split("=");
//                 String term = pair[0];
//                 int count = Integer.parseInt(pair[1]);
//                 termCountMap.put(term, count);
//                 totalTerms += count;
//             }
            
//             // Calculate TF-IDF score for each term
//             for (Map.Entry<String, Integer> entry : termCountMap.entrySet()) {
//                 String term = entry.getKey();
//                 int termFreq = entry.getValue();
//                 double tfidf = (double) termFreq / totalTerms;
//                 outputValue.set(term + "\t" + tfidf);
//                 context.write(key, outputValue);
//             }
//         }
//     }

//     public static void main(String[] args) throws Exception {
//         Configuration conf = new Configuration();
//         Job job = Job.getInstance(conf, "TF-IDF Stripes");

//         job.setJarByClass(TFIDFStripes.class);
//         job.setMapperClass(TFMapper.class);
//         job.setReducerClass(TFReducer.class);

//         job.setOutputKeyClass(Text.class);
//         job.setOutputValueClass(Text.class);

//         FileInputFormat.addInputPath(job, new Path(args[0]));
//         FileOutputFormat.setOutputPath(job, new Path(args[1]));

//         System.exit(job.waitForCompletion(true) ? 0 : 1);
//     }
// }



// import java.io.BufferedReader;
// import java.io.FileReader;
// import java.io.IOException;
// import java.net.URI;
// import java.util.HashSet;
// import java.util.Map;
// import java.util.Set;
// import java.util.TreeMap;

// import javax.naming.Context;

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.IntWritable;
// //import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.util.StringUtils;

// public class TopWords{

// 	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

// 		private final static IntWritable one = new IntWritable(1);
// 		private Text word = new Text();

// 		private Set<String> patternsToSkip = new HashSet<String>();

// 		@Override
// 		public void setup(Context context) throws IOException, InterruptedException {
// 			Configuration conf = context.getConfiguration();
// 			//caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
// 			URI[] cacheFiles = context.getCacheFiles();
//             if (cacheFiles != null && cacheFiles.length > 0) {
//               try (BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()))) {
//                 String line;
//                 while ((line = reader.readLine()) != null) {
//                   patternsToSkip.add(line.trim());
//                 }
//               }
//             }
// 		}


// 		@Override
// 		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
// 			String[] words = value.toString().toLowerCase().replaceAll("[^a-zA-Z ]", "").split("\\s+");
// 			for (String wor : words) {
// 				if(patternsToSkip.contains(wor)) continue;
// 				word.set(wor);
// 				context.write(word, one);
// 			}
// 		}
// 	}

// 	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
// 		private IntWritable result = new IntWritable();
// 		private TreeMap<Integer, Text> topWords = new TreeMap<>();

// 		public void reduce(Text key, Iterable<IntWritable> values, Context context)
// 				throws IOException, InterruptedException {
// 			int sum = 0;
// 			for (IntWritable val : values) {
// 				sum += val.get();
// 			}
// 			// Store the word and its frequency in the TreeMap
// 			topWords.put(sum, new Text(key));
// 			// Keep only the top 50 words
// 			if (topWords.size() > 50) {
// 				topWords.remove(topWords.firstKey());
// 			}
// 			// result.set(sum);
// 			// context.write(key, result);
// 		}

// 		@Override
// 		protected void cleanup(Context context) throws IOException, InterruptedException {
// 			// Emit the top 50 words with their frequencies
// 			if (topWords != null)
// 			{
// 				for (Map.Entry<Integer, Text> entry : topWords.descendingMap().entrySet()) {
// 					Text word = entry.getValue();
// 					int frequency = entry.getKey();
// 					context.write(word, new IntWritable(frequency));
// 				}
// 			}
// 		}
// 	}

// 	public static void main(String[] args) throws Exception {

// 		Configuration conf = new Configuration();
// 		Job job = Job.getInstance(conf, "wordcount2");

// 		job.setMapperClass(TokenizerMapper.class);
// 		// job.setCombinerClass(IntSumReducer.class); // enable to use 'local aggregation'
// 		job.setReducerClass(IntSumReducer.class);

// 		job.setOutputKeyClass(Text.class);
// 		job.setOutputValueClass(IntWritable.class);

//         // job.addCacheFile(new Path("/home/bhavil/Desktop/NoSQL-A2/stopwords.txt").toUri());

// 		job.addCacheFile(new Path("/testers/stopwords.txt").toUri());
		
// 		FileInputFormat.addInputPath(job, new Path(args[0]));
// 		FileOutputFormat.setOutputPath(job, new Path(args[1]));

// 		System.exit(job.waitForCompletion(true) ? 0 : 1);
// 	}
// }

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class TopWords {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Map<String, Boolean> stopWords = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load stop words from distributed cache
            URI[] stopWordFiles = context.getCacheFiles();
            for (URI stopWordFile : stopWordFiles) {
                loadStopWords(stopWordFile, context);
            }
        }

        private void loadStopWords(URI file, Context context) throws IOException {
            BufferedReader br = new BufferedReader(new FileReader(file.toString()));
            String line;
            while ((line = br.readLine()) != null) {
                stopWords.put(line.trim(), true);
            }
            br.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken().toLowerCase();
                if (!stopWords.containsKey(token)) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private PriorityQueue<Pair> queue = new PriorityQueue<>(50, (a, b) -> b.getCount() - a.getCount());

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            queue.add(new Pair(key.toString(), sum));
            if (queue.size() > 50) {
                queue.poll();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (!queue.isEmpty()) {
                Pair pair = queue.poll();
                context.write(new Text(pair.getWord()), new IntWritable(pair.getCount()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "topwords");

        job.setJarByClass(TopWords.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class); // Optional
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Add stop words file to distributed cache
        job.addCacheFile(new Path("/home/bhavil/Desktop/NoSQL-A2/stopwords.txt").toUri());

		// job.addCacheFile(new Path("/testers/stopwords.txt").toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static class Pair {
        private String word;
        private int count;

        public Pair(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public int getCount() {
            return count;
        }
    }
}
