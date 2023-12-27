package org.movieratingservice;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@Slf4j
public class MovieRateMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int POPULARITY_INDEX = 5;
    private static final int VOTE_AVERAGE_INDEX = 7;
    private static final int VOTE_COUNT_INDEX = 8;
    private static final Text movieCountKey = new Text("MovieCount");
    private static final IntWritable one = new IntWritable(1);
    private static final IntWritable zero = new IntWritable(0);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");

        // Skip header row assuming it's the first row
        if (key.get() == 0) {
            log.info("Skipping header row: {}", value);
        }

        log.info("Column count: {} | columns: {}", columns.length, String.join(",", columns));

        if (columns.length >= 9) {
            try {
                double popularity = Double.parseDouble(columns[POPULARITY_INDEX]);
                double voteAverage = Double.parseDouble(columns[VOTE_AVERAGE_INDEX]);
                int voteCount = Integer.parseInt(columns[VOTE_COUNT_INDEX]);

                if (popularity > 500.0 && voteAverage > 8.0 && voteCount > 1000) {
                    context.write(movieCountKey, one);
                } else {
                    context.write(movieCountKey, zero);
                }
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                log.error("Error processing row {}: {}", key.get(), value);
                e.printStackTrace();
                // Handle any parsing errors or missing columns
            }
        }
    }
}
