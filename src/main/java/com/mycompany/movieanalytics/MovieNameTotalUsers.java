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
public class MovieNameTotalUsers {
        public static class TotalMovieMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private Text movieName = new Text();
        private static final LongWritable ONE = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                movieName.set(parts[1]);
            context.write(movieName, ONE);
            }
        }
    }

    public static class TotalMovieReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable  value : values) {
                
                count += value.get();
                
            }
            context.write(key, new LongWritable(count));
        }
    } 
}
