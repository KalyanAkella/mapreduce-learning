import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Neighbours implements Iterable<TextArray> {

    private String text;
    private Matcher dotMatcher;

    public Neighbours(String text) {
        this.text = text.endsWith(".") ? text : text + ".";
        this.text = this.text.replaceAll("\\.+", ".");
        this.dotMatcher = Pattern.compile("\\.").matcher(this.text);
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
                if (words.size() > 0) {
                    if (currentWordIndex == 0) {
                        result = TextArray.from(words.subList(currentWordIndex + 1, words.size()));
                        currentWordIndex ++;
                    } else if (currentWordIndex >= (words.size() - 1)) {
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
                }
                return result;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
