package org.apache.lucene.demo;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Paths;

public class FieldWriterTest {

    public static void main(String[] args) throws IOException {
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig();

        Directory dir = FSDirectory.open(Paths.get("/Users/yangchenglong/data/files/index"));

//        IndexWriter indexWriter = new IndexWriter(dir, indexWriterConfig);

//        addDoc(indexWriter, "Lucene in Action", "193398817", -5, new int[]{1,2}, new String[]{"los angles", "beijing"});
//        addDoc(indexWriter, "Lucene for Dummies", "55320055Z", 4, new int[]{5,1}, new String[]{"shanghai", "beijing"});
//        addDoc(indexWriter, "Managing Gigabytes", "55063554A", 12, new int[]{0, 1, 2}, new String[]{"shenzhen", "guangzhou"});
//        addDoc(indexWriter, "The Art of Computer Science", "9900333X", 2, new int[]{10, 4, 3}, new String[]{"shanghai", "los angles"});
//        addDoc(indexWriter, "C++ Primer", "914324235", 11, new int[]{0, 5, 2, 3}, new String[]{"beijing", "shenzhen"});
//        addDoc(indexWriter, "I like Lucene", "fdsjfa2313", 1, new int[]{0, 1, 2, 4}, new String[]{"nanjing", "tianjin"});
//        addDoc(indexWriter, "Lucene and C++ Primer", "fdsfaf", 10, new int[]{0, 1, 2}, new String[]{"shenzhen", "guangzhou"});
//        addDoc(indexWriter, "C++ api", "411223432", 2, new int[]{0, 11, 2}, new String[]{"shenzhen", "shanghai"});
//        addDoc(indexWriter, "C++ Primer", "914324236", 50, new int[]{3,2,6,1}, new String[]{"beijing"});
//
//        indexWriter.close();

        // 2. query
        String querystr =  "lucene";

        // the "title" arg specifies the default field to use
        // when no field is explicitly specified in the query.
         Query q = new TermQuery(new Term("title", querystr));
        // BooleanQuery query = new BooleanQuery();
//        MatchAllDocsQuery q = new MatchAllDocsQuery();

        //sort
        SortField visitSort = new SortedNumericSortField("visit", SortField.Type.INT, true);
        Sort sort = new Sort(visitSort);

        // 3. search
        int hitsPerPage = 10;

        IndexReader reader = DirectoryReader.open(dir);

        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs docs = searcher.search(q, hitsPerPage, sort);

        ScoreDoc[] hits = docs.scoreDocs;

        // 4. display results
        System.out.println("Found " + hits.length + " hits.");

        for(int i=0;i<hits.length;++i) {
            int docId = hits[i].doc;
            Document d = searcher.doc(docId);
            System.out.println((i + 1) + ". " + d.get("isbn") + "\t" + d.get("title") + "\t" + d.get("visit"));
        }

        // reader can only be closed when there
        // is no need to access the documents any more.
        reader.close();
    }

    private static void addDoc(IndexWriter w, String title, String isbn, int visit, int [] sale_list, String []locations) throws IOException {
        Document doc = new Document();
        doc.add(new StoredField("visit", visit));
        doc.add(new TextField("title", title, Field.Store.YES));

        // use a string field for isbn because we don't want it tokenized
        doc.add(new StringField("isbn", isbn, Field.Store.YES));
        doc.add(new SortedDocValuesField("title",new BytesRef(title)));
//        if (!title.equals("C++ api")){
            doc.add(new NumericDocValuesField("visit", visit));
//        }
        for (int sale : sale_list){
            doc.add(new SortedNumericDocValuesField("sale", sale));
            doc.add(new SortedNumericDocValuesField("sale", sale));
        }

        for (String location: locations){
            doc.add(new SortedSetDocValuesField("city", new BytesRef(location)));
        }

        w.addDocument(doc);
    }
}
