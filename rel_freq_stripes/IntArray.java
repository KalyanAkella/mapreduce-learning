import org.apache.hadoop.io.IntWritable;

public class IntArray extends PrintableArrayWritable {
    public IntArray() {
        super(IntWritable.class);
    }
}
