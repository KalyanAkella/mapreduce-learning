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

public class Pairs extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, StringPair, IntWritable> {

        private HashMap<StringPair,Integer> buffer;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            buffer = new HashMap<StringPair, Integer>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            for (StringPair wordPair : new WordPairs(text)) {
                if (buffer.containsKey(wordPair)) {
                    buffer.put(wordPair, buffer.get(wordPair) + 1);
                } else {
                    buffer.put(wordPair, 1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (StringPair wordPair : buffer.keySet()) {
                context.write(wordPair, new IntWritable(buffer.get(wordPair)));
            }
        }
    }

    public static class Reduce extends Reducer<StringPair, IntWritable, StringPair, IntWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public int run(String[] args) throws Exception {
        Job job = new Job();

        job.setOutputKeyClass(StringPair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(Pairs.class);

//        job.submit();
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        ToolRunner.run(new Pairs(), otherArgs);
    }
}
