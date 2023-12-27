package org.movieratingservice.q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovieYearReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalCount = 0;

        // Count the number of movies for each year
        for (IntWritable value : values) {
            totalCount += value.get();
        }

        // Emit key-value pair: (year, total_count)
        context.write(key, new IntWritable(totalCount));
    }
}

