package org.apache.lucene.demo.trie;

import java.util.HashMap;
import java.util.Map;

public class TrieHashDemo {

    private TrieNode root = new TrieNode();

    static class TrieNode{

        public Character value;

        public Map<Character,TrieNode> children = new HashMap<>();

        private boolean isEnd;

        public TrieNode(char val){
            this.value = val;
        }

        public TrieNode(){

        }

        public void addNext(Character key, TrieNode node){
            children.put(key, node);
        }

        public TrieNode getNext(Character key){
            return children.get(key);
        }

        public boolean isLastCharacter(){
            return children.isEmpty();
        }
    }


    public void addWord(String word){
        TrieNode existNode = root;
        for(char c : word.toCharArray()){
            TrieNode node = existNode.getNext(c);

            if(null == node){
                node = new TrieNode();
                existNode.addNext(c, node);
            }
            existNode = node;
        }
    }

    public String replace(String text, String afterReplace) {
        StringBuilder result = new StringBuilder(text.length());
        TrieNode tmpNode = root;
        int begin = 0, pos = 0;
        while (pos < text.length()) {
            char c = text.charAt(pos);
            tmpNode = tmpNode.getNext(c);
            if (null == tmpNode) {
                result.append(text.charAt(begin));
                begin++;
                pos = begin;
                tmpNode = root;
            } else if (tmpNode.isLastCharacter()) {
                // 匹配完成, 进行替换
                result.append(afterReplace);
                pos++;
                begin = pos;
                tmpNode = root;
            } else {
                // 匹配上向后移
                pos++;
            }
        }
        result.append(text.substring(begin));
        return result.toString();
    }

    /**
     * 查找
     *
     * @param text 待处理文本
     * @return 统计数据 key: word value: count
     */
    public Map<String, Integer> find(String text) {
        Map<String, Integer> resultMap = new HashMap<>(16);
        TrieNode tmpNode = root;
        StringBuilder word = new StringBuilder();
        int begin = 0, pos = 0;
        while (pos < text.length()) {
            char c = text.charAt(pos);
            tmpNode = tmpNode.getNext(c);
            if (null == tmpNode) {
                begin++;
                pos = begin;
                tmpNode = root;
            } else if (tmpNode.isLastCharacter()) {
                // 匹配完成
                String w = word.append(c).toString();
                resultMap.put(w, resultMap.getOrDefault(w, 0) + 1);
                pos++;
                begin = pos;
                tmpNode = root;
                word = new StringBuilder();
            } else {
                // 匹配上向后移
                word.append(c);
                pos++;
            }
        }
        return resultMap;
    }
}
