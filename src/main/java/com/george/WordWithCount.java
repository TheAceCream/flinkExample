package com.george;

/**
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/2/27 16:44 Time: 14:15
 */
public class WordWithCount {

    private String word;
    private Long count;

    public WordWithCount(){}

    public WordWithCount(String word,Long count){
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordWithCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
