package org.apache.lucene.demo;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.CharsRef;

import java.io.IOException;

public class TokenStreamDemo {
    public static void main(String[] args) throws IOException {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                LetterTokenizer letterTokenizer = new LetterTokenizer();
                SynonymMap.Builder builder = new SynonymMap.Builder();
                // 设置hello的两个同义词：hi和ha
                builder.add(new CharsRef("hello"), new CharsRef("hi"), true);
                builder.add(new CharsRef("hello"), new CharsRef("ha"), true);

                SynonymMap synonymMap = null;
                try {
                    synonymMap = builder.build();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                SynonymGraphFilter synonymGraphFilter = new SynonymGraphFilter(letterTokenizer, synonymMap, true);
                return new TokenStreamComponents(letterTokenizer, synonymGraphFilter);
            }
        };

        TokenStream tokenStream = analyzer.tokenStream("test", "hello world,welcome to virtural");
        tokenStream.reset();
        CharTermAttribute termAttr = tokenStream.addAttribute(CharTermAttribute.class);
        OffsetAttribute offsetAttr = tokenStream.addAttribute(OffsetAttribute.class);

        while(tokenStream.incrementToken()) {
            System.out.printf("%s startOffset=%d endOffset=%d \n", termAttr.toString(), offsetAttr.startOffset(), offsetAttr.endOffset());
        }

    }
}
