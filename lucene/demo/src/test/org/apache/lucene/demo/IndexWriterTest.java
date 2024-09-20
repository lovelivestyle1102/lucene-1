package org.apache.lucene.demo;

//import com.carrotsearch.randomizedtesting.RandomizedContext;
//import com.carrotsearch.randomizedtesting.RandomizedRunner;
//import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.xml.builders.TermQueryBuilder;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.util.Version;
import org.junit.Test;
import org.junit.runner.RunWith;
//import org.junit.Test;
//import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Random;

import static org.apache.lucene.util.LuceneTestCase.newBytesRef;

@RunWith(RandomizedRunner.class)
public class IndexWriterTest {

//    @Test
    public void testFixedBitSet(){
        FixedBitSet fixedBitSet = new FixedBitSet(300);
        fixedBitSet.set(3);
        fixedBitSet.set(179);
        fixedBitSet.set(299);
        System.out.println(fixedBitSet.get(179));
        System.out.println(fixedBitSet.get(200));
        FixedBitSet fixedBitSet2 = new FixedBitSet(300);
        fixedBitSet2.set(3);
        FixedBitSet fixedBitSet3 = new FixedBitSet(300);
        fixedBitSet3.set(9);
        fixedBitSet2.or(fixedBitSet3);
        System.out.println(fixedBitSet2.get(9));
    }


    @Test
    public void testWriter() throws IOException {
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig();

        indexWriterConfig.setUseCompoundFile(false);

        //MMapDirectory
        Directory dir = FSDirectory.open(Paths.get("/Users/yangchenglong/data/files/index"));

        IndexWriter indexWriter = new IndexWriter(dir, indexWriterConfig);

//
//        Document doc = null;
//        doc = new Document();
//        doc.add(new StringField("contents","我是一个来自华夏的程序员", Field.Store.YES));
//        doc.add(new BinaryDocValuesField("binarydv", newBytesRef("500")));
//        doc.add(new NumericDocValuesField("numericdv", 500));
//        doc.add(new SortedDocValuesField("sorteddv", newBytesRef("500")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("one")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("two")));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 4));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 3));
//        indexWriter.addDocument(doc);
//
        Document doc = new Document();
        doc.add(new StringField("contents","我是一个来自未来的淘宝系的主播机器人", Field.Store.YES));
//        doc.add(new BinaryDocValuesField("binarydv", newBytesRef("501")));
//        doc.add(new NumericDocValuesField("numericdv", 501));
//        doc.add(new SortedDocValuesField("sorteddv", newBytesRef("501")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("two")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("three")));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 6));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 1));
        indexWriter.addDocument(doc);

        Document doc1 = new Document();
        doc1.add(new StringField("contents","我是一个来自淘宝系的主播人", Field.Store.YES));
//        doc.add(new BinaryDocValuesField("binarydv", newBytesRef("501")));
//        doc.add(new NumericDocValuesField("numericdv", 501));
//        doc.add(new SortedDocValuesField("sorteddv", newBytesRef("501")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("two")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("three")));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 6));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 1));
        indexWriter.addDocument(doc1);

        Document doc2 = new Document();
        doc2.add(new StringField("contents","我是一个来自天猫系的主播", Field.Store.YES));
//        doc.add(new BinaryDocValuesField("binarydv", newBytesRef("501")));
//        doc.add(new NumericDocValuesField("numericdv", 501));
//        doc.add(new SortedDocValuesField("sorteddv", newBytesRef("501")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("two")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("three")));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 6));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 1));
        indexWriter.addDocument(doc2);

        Document doc3 = new Document();
        doc3.add(new StringField("contents","我是一个来自淘宝系的主播机器人", Field.Store.YES));
//        doc.add(new BinaryDocValuesField("binarydv", newBytesRef("501")));
//        doc.add(new NumericDocValuesField("numericdv", 501));
//        doc.add(new SortedDocValuesField("sorteddv", newBytesRef("501")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("two")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("three")));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 6));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 1));
        indexWriter.addDocument(doc3);

        indexWriter.commit();
//
//
//        doc = new Document();
//        doc.add(new StringField("contents","通过\"mo\"、\"moth\"的介绍已经可以了解读取FST的逻辑", Field.Store.YES));
//        doc.add(new BinaryDocValuesField("binarydv", newBytesRef("11")));
//        doc.add(new NumericDocValuesField("numericdv", 51));
//        doc.add(new SortedDocValuesField("sorteddv", newBytesRef("11")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("two")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("three")));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 11));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 3));
//        indexWriter.addDocument(doc);
//
//        doc = new Document();
//        doc.add(new StringField("contents","那么在源码中会保留该节点前的字符信息", Field.Store.YES));
//        doc.add(new BinaryDocValuesField("binarydv", newBytesRef("91")));
//        doc.add(new NumericDocValuesField("numericdv", 331));
//        doc.add(new SortedDocValuesField("sorteddv", newBytesRef("221")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("five")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("three")));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 3));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 6));
//        indexWriter.addDocument(doc);
//
//        doc = new Document();
//        doc.add(new StringField("contents","我是一个来自淘宝系的主播", Field.Store.YES));
//        doc.add(new BinaryDocValuesField("binarydv", newBytesRef("343")));
//        doc.add(new NumericDocValuesField("numericdv", 111));
//        doc.add(new SortedDocValuesField("sorteddv", newBytesRef("112")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("four")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("six")));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 6));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 13));
//        indexWriter.addDocument(doc);
//
//        doc = new Document();
//        doc.add(new StringField("contents","从FST信息中读取该term的附加值", Field.Store.YES));
//        doc.add(new BinaryDocValuesField("binarydv", newBytesRef("632")));
//        doc.add(new NumericDocValuesField("numericdv", 563));
//        doc.add(new SortedDocValuesField("sorteddv", newBytesRef("234")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("seven")));
//        doc.add(new SortedSetDocValuesField("sortedsetdv", newBytesRef("ten")));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 10));
//        doc.add(new SortedNumericDocValuesField("sortednumericdv", 8));
//        indexWriter.addDocument(doc);
//
//        indexWriter.commit();
//          Document doc = null;
//          doc = new Document();
//          doc.add(new TextField("content","h",Field.Store.YES));
//          indexWriter.addDocument(doc);
//
//        doc = new Document();
//        doc.add(new TextField("content","b",Field.Store.YES));
//        indexWriter.addDocument(doc);
//
//        doc = new Document();
//        doc.add(new TextField("content","a c",Field.Store.YES));
//        indexWriter.addDocument(doc);
//
//        doc = new Document();
//        doc.add(new TextField("content","a c e",Field.Store.YES));
//        indexWriter.addDocument(doc);
//
//        doc = new Document();
//        doc.add(new TextField("content","h",Field.Store.YES));
//        indexWriter.addDocument(doc);
//
//        doc = new Document();
//        doc.add(new TextField("content","i",Field.Store.YES));
//        indexWriter.addDocument(doc);
//
//        doc = new Document();
//        doc.add(new TextField("content","c a e",Field.Store.YES));
//        indexWriter.addDocument(doc);
//
//        doc = new Document();
//        doc.add(new TextField("content","f",Field.Store.YES));
//        indexWriter.addDocument(doc);
//
//        doc = new Document();
//        doc.add(new TextField("content","b c d e c e",Field.Store.YES));
//        indexWriter.addDocument(doc);
//
//        doc = new Document();
//        doc.add(new TextField("content","a c e a b c",Field.Store.YES));
//        indexWriter.addDocument(doc);
//
//        indexWriter.deleteDocuments(new Term("content","h"));
//        indexWriter.deleteDocuments(new Term("content","f"));
//        indexWriter.commit();


    }

    @Test
    public void testSearcher() throws IOException, ParseException {
        DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get("/Users/yangchenglong/data/files/index")));

        IndexSearcher searcher = new IndexSearcher(reader);
//
//        Analyzer analyzer = new StandardAnalyzer();
//
//        TokenStream tokenStream = analyzer.tokenStream("", "");
//
//        tokenStream.incrementToken();
//
//        QueryParser parser = new QueryParser("contents", analyzer);
//
//        //根据查询语句生成的是一个Query树
//        Query parseQuery = parser.parse("华夏");

        TermQuery termQuery = new TermQuery(new Term("contents", "我是一个来自华夏的程序员"));

        TopDocs topDocs = searcher.search(termQuery, 100);

        for(ScoreDoc scoreDocs: topDocs.scoreDocs) {
//            System.out.println(scoreDocs.toString());
            Document doc = searcher.doc(scoreDocs.doc);
            System.out.println(doc);
        }
    }

//    /**
//     * Creates a {@link BytesRef} holding UTF-8 bytes for the incoming String, that sometimes uses a
//     * non-zero {@code offset}, and non-zero end-padding, to tickle latent bugs that fail to look at
//     * {@code BytesRef.offset}.
//     */
//    public static BytesRef newBytesRef(String s) {
//        return newBytesRef(s.getBytes(StandardCharsets.UTF_8));
//    }
//
//    /**
//     * Creates a copy of the incoming {@link BytesRef} that sometimes uses a non-zero {@code offset},
//     * and non-zero end-padding, to tickle latent bugs that fail to look at {@code BytesRef.offset}.
//     */
//    public static BytesRef newBytesRef(BytesRef b) {
//        assert b.isValid();
//        return newBytesRef(b.bytes, b.offset, b.length);
//    }
//
//    /**
//     * Creates a random BytesRef from the incoming bytes that sometimes uses a non-zero {@code
//     * offset}, and non-zero end-padding, to tickle latent bugs that fail to look at {@code
//     * BytesRef.offset}.
//     */
//    public static BytesRef newBytesRef(byte[] b) {
//        return newBytesRef(b, 0, b.length);
//    }
//
//    /**
//     * Creates a random empty BytesRef that sometimes uses a non-zero {@code offset}, and non-zero
//     * end-padding, to tickle latent bugs that fail to look at {@code BytesRef.offset}.
//     */
//    public static BytesRef newBytesRef() {
//        return newBytesRef(new byte[0], 0, 0);
//    }
//
//    /**
//     * Creates a random empty BytesRef, with at least the requested length of bytes free, that
//     * sometimes uses a non-zero {@code offset}, and non-zero end-padding, to tickle latent bugs that
//     * fail to look at {@code BytesRef.offset}.
//     */
//    public static BytesRef newBytesRef(int byteLength) {
//        return newBytesRef(new byte[byteLength], 0, byteLength);
//    }
//
//    /**
//     * Creates a copy of the incoming bytes slice that sometimes uses a non-zero {@code offset}, and
//     * non-zero end-padding, to tickle latent bugs that fail to look at {@code BytesRef.offset}.
//     */
//    public static BytesRef newBytesRef(byte[] bytesIn, int offset, int length) {
//        // System.out.println("LTC.newBytesRef!  bytesIn.length=" + bytesIn.length + " offset=" + offset
//        //                 + " length=" + length);
//
//        assert bytesIn.length >= offset + length
//                : "got offset=" + offset + " length=" + length + " bytesIn.length=" + bytesIn.length;
//
//        // randomly set a non-zero offset
//        int startOffset;
//        if (random().nextBoolean()) {
//            startOffset = RandomNumbers.randomIntBetween(random(), 1, 20);
//        } else {
//            startOffset = 0;
//        }
//
//        // also randomly set an end padding:
//        int endPadding;
//        if (random().nextBoolean()) {
//            endPadding = RandomNumbers.randomIntBetween(random(), 1, 20);
//        } else {
//            endPadding = 0;
//        }
//
//        byte[] bytes = new byte[startOffset + length + endPadding];
//
//        System.arraycopy(bytesIn, offset, bytes, startOffset, length);
//        // System.out.println("LTC:  return bytes.length=" + bytes.length + " startOffset=" +
//        //                 startOffset + " length=" + length);
//
//        BytesRef it = new BytesRef(bytes, startOffset, length);
//        assert it.isValid();
//
//        if (RandomNumbers.randomIntBetween(random(), 1, 17) == 7) {
//            // try to ferret out bugs in this method too!
//            return newBytesRef(it.bytes, it.offset, it.length);
//        }
//
//        return it;
//    }
//
//    public static Random random() {
//        return RandomizedContext.current().getRandom();
//    }
}
