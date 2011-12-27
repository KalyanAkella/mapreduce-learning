import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordPairs implements Iterable<StringPair> {

    private String text;
    private Matcher whiteSpaceMatcher;

    public WordPairs(String text) {
        this.text = text;
        this.whiteSpaceMatcher = Pattern.compile("\\s+").matcher(text);
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
