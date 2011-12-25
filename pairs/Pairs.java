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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Pairs extends Configured implements Tool {

    public static class StringPair implements WritableComparable<StringPair> {
        private String first;
        private String second;

        public StringPair() {
        }

        public StringPair(String first, String second) {
            set(first, second);
        }

        public void set(String left, String right) {
            first = left;
            second = right;
        }

        public String getFirst() {
            return first;
        }

        public String getSecond() {
            return second;
        }

        public void readFields(DataInput in) throws IOException {
            String[] strings = WritableUtils.readStringArray(in);
            first = strings[0];
            second = strings[1];
        }

        public void write(DataOutput out) throws IOException {
            String[] strings = new String[] { first, second };
            WritableUtils.writeStringArray(out, strings);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("StringPair");
            sb.append("{first='").append(first).append('\'');
            sb.append(", second='").append(second).append('\'');
            sb.append('}');
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StringPair that = (StringPair) o;

            if (first != null ? !first.equals(that.first) : that.first != null) return false;
            if (second != null ? !second.equals(that.second) : that.second != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = first != null ? first.hashCode() : 0;
            result = 31 * result + (second != null ? second.hashCode() : 0);
            return result;
        }

        public static class Comparator extends WritableComparator {
            public Comparator() {
                super(StringPair.class);
            }

            public int compare(byte[] b1, int s1, int l1,
                               byte[] b2, int s2, int l2) {
                return compareBytes(b1, s1, l1, b2, s2, l2);
            }
        }

        // register this comparator
        static {
            WritableComparator.define(StringPair.class, new Comparator());
        }

        public int compareTo(StringPair o) {
            if (first == null && o.first != null) {
                return -1;
            } else if (first != null && o.first == null) {
                return 1;
            } else if (first != null && o.first != null) {
                return first.compareTo(o.first);
            } else {
                if (second == null && o.second != null) {
                    return -1;
                } else if (second != null && o.second == null) {
                    return 1;
                } else if (second != null && o.second != null) {
                    return second.compareTo(o.second);
                } else {
                    return 0;
                }
            }
        }
    }

    public static class WordPairs implements Iterable<StringPair> {
        private final Pattern whiteSpacePattern = Pattern.compile("\\s+");

        private String text;
        private Matcher whiteSpaceMatcher;

        public WordPairs(String text) {
            this.text = text;
            this.whiteSpaceMatcher = whiteSpacePattern.matcher(text);
        }

        public Iterator<StringPair> iterator() {

            return new Iterator<StringPair>() {

                private int currentIndex = 0;
                private String previousWord;

                public boolean hasNext() {
                    return !whiteSpaceMatcher.hitEnd() || currentIndex < text.length();
                }

                public StringPair next() {
                    if (previousWord == null) {
                        if (whiteSpaceMatcher.find()) {
                            previousWord = text.substring(currentIndex, whiteSpaceMatcher.start());
                            currentIndex = whiteSpaceMatcher.end();
                        }
                    }

                    String nextWord = null;
                    if (whiteSpaceMatcher.find()) {
                        nextWord = text.substring(currentIndex, whiteSpaceMatcher.start());
                        currentIndex = whiteSpaceMatcher.end();
                    }

                    if (nextWord == null && currentIndex < text.length()) {
                        nextWord = text.substring(currentIndex);
                        currentIndex = text.length();
                    }

                    StringPair result = new StringPair(previousWord, nextWord);
                    previousWord = nextWord;
                    return result;
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    public static class Map extends Mapper<LongWritable, Text, StringPair, IntWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            for (StringPair wordPair : new WordPairs(text)) {
                context.write(wordPair, new IntWritable(1));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
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
