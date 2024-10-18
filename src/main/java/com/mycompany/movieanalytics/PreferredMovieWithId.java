/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.hproject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
/**
 *
 * @author perso
 */
public class PreferredMovieWithId {

public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text userId = new Text();
    private Text movieRating = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Split the input line into fields using newline character
        String[] lines = value.toString().split("\n");

        for (String line : lines) {
            // Split each line into fields using commas
            String[] fields = line.split(",");

            // Check if the line has the correct number of fields
            if (fields.length == 4) {
                // Extract userId and movieRating
                userId.set(fields[0]);
                
                // Check if fields[2] is a valid double value
                try {
                    Double.parseDouble(fields[2]);
                } catch (NumberFormatException e) {
                    // Log or handle the error
                    context.getCounter("Error Counters", "Invalid Rating").increment(1);
                    return;
                }
                
                movieRating.set(fields[1] + "," + fields[2]);  // Note the change in field indices

                // Emit (userId, movieId,rating) pair
                context.write(userId, movieRating);
            } else {
                // Log or handle the error (unexpected number of fields)
                context.getCounter("Error Counters", "Invalid Line Format").increment(1);
            }
        }
    }
}


public static class Reduce extends Reducer<Text, Text, Text, Text> {
    private Text preferredMovie = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Variables to store the highest rated movie for each user
        String highestRatedMovie = "";
        double highestRating = Double.MIN_VALUE;

        // Iterate over all values (movies with ratings) for a specific user
        for (Text value : values) {
            String[] fields = value.toString().split(",");
            String movieId = fields[0];
            double rating = Double.parseDouble(fields[1]);

            // Update the highest rated movie if necessary
            if (rating >= highestRating) {
                highestRating = rating;
                highestRatedMovie = movieId;
            }
        }

        // Emit (userId, highestRatedMovie) pair
        context.write(key, new Text(highestRatedMovie));

    }
}

}
//hadoop jar hadoop/labs/hproject/target/hproject-1.0-SNAPSHOT.jar com.mycompany.hproject.Hproject ratings.csv UserIdMovieWithName
//hdfs dfs -rm -r /user/root/UserPreferredMovies
//hdfs dfs -copyFromLocal hadoop/labs/hproject/target/ratings.csv /user/root/
//hdfs dfs -mkdir -p /user/root/UserPreferredMovies
//hdfs dfs -rm -r /user/root/UserPreferredMovies
//hdfs dfs -copyToLocal /user/root/UserPreferredMovies /hadoop/labs
//hdfs dfs -rm -r /user/root/UserIdMovieWithName
//hadoop jar hadoop/labs/hproject/target/hproject-1.0-SNAPSHOT.jar com.mycompany.hproject.Hproject movies.csv UserPreferredMovies/part-r-00000 UserIdMovieWithName 

//hdfs dfs -mkdir -p /user/root/UserPreferredMovies
//hdfs dfs -copyFromLocal hadoop/labs/UserPreferredMovies /user/root/