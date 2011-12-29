import org.apache.hadoop.io.Text;

import java.util.List;

public class TextArray extends PrintableArrayWritable {
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
