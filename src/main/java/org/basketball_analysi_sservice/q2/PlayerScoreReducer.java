package org.basketball_analysi_sservice.q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PlayerScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final Text mostScoringPlayer = new Text();
    private int highestScore = 0;

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalPoints = 0;

        for (IntWritable value : values) {
            totalPoints += value.get();
        }

        // Check if this player has the highest score
        if (totalPoints > highestScore) {
            highestScore = totalPoints;
            mostScoringPlayer.set(key);
        }

        // Emit the player's total score for debugging or intermediate results
        context.write(key, new IntWritable(totalPoints));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit the most scoring player and their score
        context.write(new Text("Most Scoring Player: " + mostScoringPlayer.toString()), new IntWritable(highestScore));
    }
}
