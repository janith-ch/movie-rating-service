package org.movieratingservice.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ScoringQuarterMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");

        // Skip header row
        if (columns[0].equals("EVENTID")) return;

        try {
            String period = columns[5].trim();
            String team = columns[8].trim(); // PLAYER1_TEAM_ABBREVIATION
            String score = columns[23].trim(); // SCORE

            if (!team.isEmpty() && !score.isEmpty() && !period.isEmpty()) {
                String[] scoreParts = score.split(" - ");
                int points = 0;

                // Determine points scored in this play
                if (scoreParts.length == 2) {
                    int homeScore = Integer.parseInt(scoreParts[0].trim());
                    int awayScore = Integer.parseInt(scoreParts[1].trim());
                    points = Math.abs(homeScore - awayScore);
                }

                // Emit key as "Team_Quarter" and points as value
                context.write(new Text(team + "_" + period), new IntWritable(points));
            }
        } catch (Exception e) {
            System.err.println("Error processing row: " + value);
        }
    }
}
