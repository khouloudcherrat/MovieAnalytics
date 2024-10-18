/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.hproject;

/**
 *
 * @author perso
 */
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JoinOperation {
    // Job 2: Join Operation
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text keyText = new Text();
        private Text valueText = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input line from Job 1's output and emit <userID, movieID>
            String[] fields = value.toString().split("\t");
            if (fields.length >= 2) {
                keyText.set(fields[0]);  // userID
                valueText.set(fields[1]);  // movieID
                context.write(keyText, valueText);
            } else {
                // Handle the case where there are not enough fields in the line
                // You might want to log a warning or skip the record
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private Map<String, String> movieNames = new HashMap<>();

        @Override
protected void setup(Context context) throws IOException, InterruptedException {
    // Load movies dataset into memory using DistributedCache
    URI[] cacheFiles = context.getCacheFiles();
    if (cacheFiles != null && cacheFiles.length > 0) {
        // Load movieID and movieName into a map
        BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].getPath()));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split(",");
            //movieNames.put("only zero", String.valueOf(fields.length));
            if (fields.length >= 2) {
                movieNames.put(fields[0], fields[1]);
               
            }
        }
        reader.close();
    }
}


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Find the highest rated movie for the user and emit <userID, highestRatedMovieName>
//            Entry<String, Double> maxEntry = null;
//
//            // Iterate through the values (movieIDs)
//            for (Text value : values) {
//                String movieID = value.toString();
//
//                // Assuming you want only one movie per user, break after finding the first match
//                if (movieNames.containsKey(movieID)) {
//                    String movieName = movieNames.get(movieID);
//                    result.set(movieName);
//                    context.write(key, result);
//                    break;
//                }
//            }

        if (movieNames.isEmpty()) {
                String randomKey = "randomKey";
                String randomValue = "randomValue";
                context.write(new Text(randomKey), new Text(randomValue));
            } else {
                for (Map.Entry<String, String> entry : movieNames.entrySet()) {
                    context.write(new Text(entry.getKey()), new Text(entry.getValue()));
                }
            }
            
                
      }
    }
}

