package org.apache.lucene.analysis.standard;

import org.apache.lucene.analysis.Analyzer;

public class WorkerAnalyzer extends Analyzer {

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        return null;
    }
}
