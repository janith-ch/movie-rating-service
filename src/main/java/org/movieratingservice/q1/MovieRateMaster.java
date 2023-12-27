package org.movieratingservice.q1;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


@Slf4j
public class MovieRateMaster {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            log.error("Usage: MovieCriteriaCountDriver <inputPath> <outputPath>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Criteria Count");

        job.setJarByClass(MovieRateMaster.class);
        job.setMapperClass(MovieRateMapper.class);
        job.setReducerClass(MovieRateReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}