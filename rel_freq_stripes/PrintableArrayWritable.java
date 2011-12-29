import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class PrintableArrayWritable extends ArrayWritable {

    public PrintableArrayWritable(Class<? extends Writable> valueClass) {
        super(valueClass);
    }

    public PrintableArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
        super(valueClass, values);
    }

    public PrintableArrayWritable(String[] strings) {
        super(strings);
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder("[");
        for (Writable value : this.get()) {
            String valueString = value.toString();
            buffer.append(valueString).append(",");
        }
        buffer.append("]");
        return buffer.toString();
    }

}
