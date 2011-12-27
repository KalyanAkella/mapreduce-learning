import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
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
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Stripes extends Configured implements Tool {

    public static class TextArray extends ArrayWritable {
        public TextArray() {
            super(Text.class);
        }

        public static TextArray from(List<String> strings) {
            TextArray textArray = new TextArray();
            Text[] values = new Text[strings.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = new Text(strings.get(i));
            }
            textArray.set(values);
            return textArray;
        }
    }

    public static class Neighbours implements Iterable<TextArray> {
        private final Pattern dotPattern = Pattern.compile("\\.");

        private String text;
        private Matcher dotMatcher;

        public Neighbours(String text) {
            this.text = text.endsWith(".") ? text : text + ".";
            this.text = this.text.replaceAll("\\.+", ".");
            this.dotMatcher = dotPattern.matcher(this.text);
        }

        public Iterator<TextArray> iterator() {
            return new Iterator<TextArray>() {

                private int currentIndex = 0, currentWordIndex = 0;
                private List<String> words = new ArrayList<String>();

                public boolean hasNext() {
                    return !dotMatcher.hitEnd() && currentIndex < text.length();
                }

                public TextArray next() {
                    TextArray result = null;
                    if (words.size() == 0 && dotMatcher.find()) {
                        String sentence = text.substring(currentIndex, dotMatcher.start());
                        StringTokenizer tokenizer = new StringTokenizer(sentence);
                        while (tokenizer.hasMoreTokens()) {
                            words.add(tokenizer.nextToken());
                        }
                    }
                    if (currentWordIndex == 0) {
                        result = TextArray.from(words.subList(currentWordIndex + 1, words.size()));
                        currentWordIndex ++;
                    } else if (currentWordIndex == words.size() - 1) {
                        result = TextArray.from(words.subList(0, currentWordIndex));
                        currentWordIndex = 0;
                        currentIndex = dotMatcher.end();
                        words.clear();
                    } else {
                        ArrayList<String> strings = new ArrayList<String>();
                        strings.addAll(words.subList(0, currentWordIndex));
                        strings.addAll(words.subList(currentWordIndex + 1, words.size()));
                        result = TextArray.from(strings);
                        currentWordIndex ++;
                    }
                    return result;
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class Reduce extends Reducer<Text, MapWritable, Text, MapWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
    
    public int run(String[] args) throws Exception {
        Job job = new Job();

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

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
