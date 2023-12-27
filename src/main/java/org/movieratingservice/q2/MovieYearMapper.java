package org.movieratingservice.q2;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@Slf4j
public class MovieYearMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Text movieYear = new Text();
    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");

        if (columns.length >= 7) { // Assuming release_date is at index 6
            String releaseDate = columns[6];

            // Extracting the year (assuming the date format is MM/DD/YYYY)
            String[] dateParts = releaseDate.split("/");
            if (dateParts.length >= 3) {
                String year = dateParts[2];

                // Emit key-value pair: (year, 1)
                movieYear.set(year);
                context.write(movieYear, one);
            }
        }
    }
}
