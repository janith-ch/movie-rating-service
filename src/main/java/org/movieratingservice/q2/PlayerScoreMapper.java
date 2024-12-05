package org.movieratingservice.q2;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@Slf4j
public class PlayerScoreMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");

        // Skip header row
        if (columns[0].equals("EVENTID")) return;

        try {
            String playerName = columns[7].trim(); // PLAYER1_NAME
            String score = columns[23].trim(); // SCORE

            if (!playerName.isEmpty() && !score.isEmpty()) {
                String[] scoreParts = score.split(" - ");
                int points = 0;

                // Determine points scored in this play
                if (scoreParts.length == 2) {
                    int homeScore = Integer.parseInt(scoreParts[0].trim());
                    int awayScore = Integer.parseInt(scoreParts[1].trim());
                    points = Math.abs(homeScore - awayScore);
                }

                // Emit player's name as key and points as value
                context.write(new Text(playerName), new IntWritable(points));
            }
        } catch (Exception e) {
            log.error("Error processing row: " + value);
        }
    }
}
