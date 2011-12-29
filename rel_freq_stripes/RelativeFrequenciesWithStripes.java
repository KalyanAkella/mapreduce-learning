import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

public class RelativeFrequenciesWithStripes extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, Stripe> {

        private HashMap<String, Stripe> buffer;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            buffer = new HashMap<String, Stripe>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(text);
            for (TextArray textArray : new Neighbours(text)) {
                if (tokenizer.hasMoreTokens() && textArray != null) {
                    Stripe mapWritable = constructStripe(textArray);
                    String word = tokenizer.nextToken();
                    populateBuffer(mapWritable, word);
                }
            }
        }

        private void populateBuffer(Stripe mapWritable, String word) {
            if (buffer.containsKey(word)) {
                buffer.get(word).add(mapWritable);
            } else {
                buffer.put(word, mapWritable);
            }
        }

        private Stripe constructStripe(TextArray textArray) {
            Stripe mapWritable = new Stripe();
            for (Writable neighbour : textArray.get()) {
                if (mapWritable.containsKey(neighbour)) {
                    mapWritable.put(neighbour, new IntWritable(((IntWritable) mapWritable.get(neighbour)).get() + 1));
                } else {
                    mapWritable.put(neighbour, new IntWritable(1));
                }
            }
            return mapWritable;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String word : buffer.keySet()) {
                context.write(new Text(word), buffer.get(word));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Stripe, StringPairArray, IntArray> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Stripe> values, Context context) throws IOException, InterruptedException {
            Stripe marginalBuffer = computeMarginalBufer(values);
            int marginal = computeMarginal(marginalBuffer);

            ArrayList<StringPair> pairs = new ArrayList<StringPair>();
            ArrayList<IntWritable> frequencies = new ArrayList<IntWritable>();
            for (java.util.Map.Entry<Writable, Writable> entry : marginalBuffer.entrySet()) {
                Text word = (Text) entry.getKey();
                IntWritable count = (IntWritable) entry.getValue();
                frequencies.add(new IntWritable(count.get() / marginal));
                pairs.add(new StringPair(key.toString(), word.toString()));
            }

            StringPairArray resultPairs = new StringPairArray();
            resultPairs.set(pairs.toArray(new Writable[pairs.size()]));

            IntArray resultFrequencies = new IntArray();
            resultFrequencies.set(frequencies.toArray(new Writable[frequencies.size()]));

            context.write(resultPairs, resultFrequencies);
        }

        private int computeMarginal(Stripe marginalBuffer) {
            int marginal = 0;
            for (Writable valueWritable : marginalBuffer.values()) {
                marginal += ((IntWritable) valueWritable).get();
            }
            return marginal;
        }

        private Stripe computeMarginalBufer(Iterable<Stripe> values) {
            Stripe marginalBuffer = new Stripe();
            for (Stripe value : values) {
                marginalBuffer.add(value);
            }
            return marginalBuffer;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public int run(String[] args) throws Exception {
        Job job = new Job();

        job.setOutputKeyClass(StringPairArray.class);
        job.setOutputValueClass(IntArray.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(RelativeFrequenciesWithStripes.class);

//        job.submit();
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        ToolRunner.run(new RelativeFrequenciesWithStripes(), otherArgs);
    }
}
