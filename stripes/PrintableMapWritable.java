import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class PrintableMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder("{");
        for (Writable key : keySet()) {
            buffer.append(key).append(":").append(get(key)).append(",");
        }
        return buffer.append("}").toString();
    }
}
