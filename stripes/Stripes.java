import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
import java.util.HashMap;
import java.util.StringTokenizer;

public class Stripes extends Configured implements Tool {

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

        private void populateBuffer(Stripe stripe, String word) {
            if (buffer.containsKey(word)) {
                buffer.get(word).add(stripe);
            } else {
                buffer.put(word, stripe);
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

    public static class Reduce extends Reducer<Text, Stripe, Text, Stripe> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Stripe> values, Context context) throws IOException, InterruptedException {
            Stripe stripe = new Stripe();
            for (Stripe neighbourCounts : values) {
                stripe.add(neighbourCounts);
            }
            context.write(key, stripe);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
    
    public int run(String[] args) throws Exception {
        Job job = new Job();

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Stripe.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(Stripes.class);

//        job.submit();
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        ToolRunner.run(new Stripes(), otherArgs);
    }

    private static void testNeighbours() {
        String text = "Returns a view of the portion of this list between the specified fromIndex inclusive and toIndex exclusive. (If fromIndex and toIndex are equal the returned list is empty.) The returned list is backed by this list so non-structural changes in the returned list are reflected in this list and vice-versa. The returned list supports all of the optional list operations supported by this list.";
        for (TextArray textArray : new Neighbours(text)) {
            for (String s : textArray.toStrings()) {
                System.out.print(s + ",");
            }
            System.out.println();
        }
    }

}
