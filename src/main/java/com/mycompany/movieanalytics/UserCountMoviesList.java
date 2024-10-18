/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.hproject;

/**
 *
 * @author perso
 */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class UserCountMoviesList {
    public static class MovieLikeCountSortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private Text movieName = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                movieName.set(parts[0]);
                long count = Long.parseLong(parts[1]);
                context.write(new LongWritable(count), movieName);
            }
        }
    }

    public static class MovieLikeCountSortReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        StringBuilder movieList = new StringBuilder();

        for (Text value : values) {
            if (movieList.length() > 0) {
                movieList.append(", ");
            }
            movieList.append(value.toString());
        }

        context.write(key, new Text(movieList.toString()));
        }
    }   
}
