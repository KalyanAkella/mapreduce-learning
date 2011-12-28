import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringPair implements WritableComparable<StringPair> {
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
