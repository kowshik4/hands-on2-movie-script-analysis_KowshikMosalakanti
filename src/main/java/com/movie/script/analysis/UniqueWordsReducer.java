package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class UniqueWordsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> uniqueWords = new HashSet<>();
        for (Text value : values) {
            uniqueWords.add(value.toString());
        }
        StringBuilder uniqueWordsList = new StringBuilder();
        for (String word : uniqueWords) {
            uniqueWordsList.append(word).append(", ");
        }
        context.write(key, new Text(uniqueWordsList.toString()));
    }
}
