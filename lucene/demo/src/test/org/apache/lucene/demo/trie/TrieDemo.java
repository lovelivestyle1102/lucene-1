package org.apache.lucene.demo.trie;

public class TrieDemo {

    public static void main(String[] args) {
        Trie trie = new Trie();
        trie.insert("yangchenglong");
        trie.insert("work");
        trie.insert("word");
        trie.insert("dictory");
        trie.insert("dity");
        trie.insert("day");

        boolean yang = trie.search("work");

        System.out.println("search yang result is :"+ yang);
    }

    static class Trie{
        private Trie[] children;
        private boolean isEnd;
        private char value;

        public Trie(){
            children = new Trie[26];
            isEnd = false;
        }

        public void insert(String word){
            Trie node = this;

            for(int i = 0;i < word.length();i++){
                char ch = word.charAt(i);

                int index = ch - 'a';

                if(node.children[index] == null){
                    node.children[index] = new Trie();
                }

                node = node.children[index];
            }

            node.isEnd = true;
        }

        public boolean search(String word){
            Trie node = searchPrefix(word);

            return node != null && node.isEnd;
        }

        public boolean startsWith(String prefix){
            return searchPrefix(prefix) != null;
        }

        private Trie searchPrefix(String prefix){
            Trie node = this;
            for(int i = 0; i < prefix.length(); i++){
                char ch = prefix.charAt(i);

                int index = ch - 'a';

                if(node.children[index] == null){
                    return null;
                }

                node = node.children[index];
            }
            return node;
        }
    }

}
