/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.hproject;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 *
 * @author perso
 */
public class SortedFilePerMovieName {
        public static class MovieSortMapper extends Mapper<LongWritable, Text, Text, Text> {
            private Text movieName = new Text();
            private Text userId = new Text();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] parts = value.toString().split("\t");
                if (parts.length == 2) {
                    userId.set(parts[0]);
                    movieName.set(parts[1]);
                    context.write(movieName, userId);
                }
            }
        }
        public static class MovieSortReducer extends Reducer<Text, Text, Text, Text> {
            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {
                for (Text value : values) {
                    context.write(value, key);
                }
            }
        }    
}
