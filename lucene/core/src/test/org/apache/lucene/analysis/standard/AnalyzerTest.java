package org.apache.lucene.analysis.standard;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.junit.Test;

public class AnalyzerTest {

    @Test
    public void testRandomStrings() throws Exception {
        Analyzer analyzer = new StandardAnalyzer();

//    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
        TokenStream tokenStream = analyzer.tokenStream("content", "这个是一个字符串");

//    analyzer.close();
        tokenStream.reset();

        while(tokenStream.incrementToken()){
            System.out.println(tokenStream);
        }
    }
}
