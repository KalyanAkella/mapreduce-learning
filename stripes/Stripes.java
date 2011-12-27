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

    private static void add(MapWritable to, MapWritable from) {
        for (Writable fromKey : from.keySet()) {
            if (to.containsKey(fromKey)) {
                to.put(fromKey, new IntWritable(((IntWritable) to.get(fromKey)).get() + ((IntWritable) from.get(fromKey)).get()));
            } else {
                to.put(fromKey, from.get(fromKey));
            }
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, PrintableMapWritable> {

        private HashMap<String,PrintableMapWritable> buffer;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            buffer = new HashMap<String, PrintableMapWritable>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(text);
            for (TextArray textArray : new Neighbours(text)) {
                if (tokenizer.hasMoreTokens() && textArray != null) {
                    String word = tokenizer.nextToken();
                    PrintableMapWritable mapWritable = new PrintableMapWritable();
                    Writable[] neighbours = textArray.get();
                    for (Writable neighbour : neighbours) {
                        if (mapWritable.containsKey(neighbour)) {
                            mapWritable.put(neighbour, new IntWritable(((IntWritable) mapWritable.get(neighbour)).get() + 1));
                        } else {
                            mapWritable.put(neighbour, new IntWritable(1));
                        }
                    }
                    if (buffer.containsKey(word)) {
                        add(buffer.get(word), mapWritable);
                    } else {
                        buffer.put(word, mapWritable);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String word : buffer.keySet()) {
                context.write(new Text(word), buffer.get(word));
            }
        }
    }

    public static class Reduce extends Reducer<Text, PrintableMapWritable, Text, PrintableMapWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<PrintableMapWritable> values, Context context) throws IOException, InterruptedException {
            PrintableMapWritable mapWritable = new PrintableMapWritable();
            for (PrintableMapWritable neighbourCounts : values) {
                add(mapWritable, neighbourCounts);
            }
            context.write(key, mapWritable);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
    
    public int run(String[] args) throws Exception {
        Job job = new Job();

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PrintableMapWritable.class);

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
