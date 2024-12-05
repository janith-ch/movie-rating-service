package org.movieratingservice.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ScoringQuarterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalPoints = 0;

        for (IntWritable value : values) {
            totalPoints += value.get();
        }

        context.write(key, new IntWritable(totalPoints));
    }
}
