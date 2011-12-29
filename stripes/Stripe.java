import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class Stripe extends MapWritable {

    public void add(Stripe from) {
        for (Writable fromKey : from.keySet()) {
            if (containsKey(fromKey)) {
                put(fromKey, new IntWritable(((IntWritable) get(fromKey)).get() + ((IntWritable) from.get(fromKey)).get()));
            } else {
                put(fromKey, from.get(fromKey));
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder("{");
        for (Writable key : keySet()) {
            buffer.append(key).append(":").append(get(key)).append(",");
        }
        return buffer.append("}").toString();
    }
}
