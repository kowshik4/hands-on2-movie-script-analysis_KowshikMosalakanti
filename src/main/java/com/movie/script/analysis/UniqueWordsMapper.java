package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        String characterName = "";
        HashSet<String> uniqueWords = new HashSet<>();
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if (token.startsWith("<b>") && token.endsWith("</b>")) {
                characterName = token.substring(3, token.length() - 4);
            } else {
                if (!uniqueWords.contains(token)) {
                    uniqueWords.add(token);
                    character.set(characterName);
                    word.set(token);
                    context.write(character, word);
                }
            }
        }
    }
}
