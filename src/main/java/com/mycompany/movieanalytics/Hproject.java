/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package com.mycompany.hproject;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser; 

/**
 *
 * @author perso
 */
public class Hproject {

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] files=new GenericOptionsParser(conf,args).getRemainingArgs();
    
        //FIRST JOB       
       /*Job job = Job.getInstance(conf, "UserPreferredMovies");
        job.setJarByClass(Hproject.class);
        Path input=new Path(files[0]); 
        Path output=new Path(files[1]); 
        // Set the Mapper and Reducer classes
        job.setMapperClass(PreferredMovieWithId.MyMapper.class);
        job.setReducerClass(PreferredMovieWithId.Reduce.class);
        // Set the output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Set the input and output paths
        FileInputFormat.addInputPath(job, input); 
        FileOutputFormat.setOutputPath(job, output); 
         // Wait for the job to complete and print whether it was successful or not
        System.exit(job.waitForCompletion(true) ? 0 : 1);*/
        
        //Second job : 
        /*Job job2 = Job.getInstance(conf, "JoinOperation");
       job2.setJarByClass(Hproject.class);
        Path outputjob1=new Path(files[0]); 
        Path output=new Path(files[1]); 
        job2.setMapperClass(JoinOperation.JoinMapper.class);
        job2.setReducerClass(JoinOperation.JoinReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.addCacheFile(new URI(new Path(files[2]).toString()));
        FileInputFormat.addInputPath(job2, outputjob1); 
        FileOutputFormat.setOutputPath(job2, output); 
        job2.waitForCompletion(true);*/
        
        //Third job : sort the file according to movie name
       /* Job job = Job.getInstance(conf, "Movie Sort");  // Updated Job name to "job3"
        job.setJarByClass(Hproject.class);
        job.setMapperClass(SortedFilePerMovieName.MovieSortMapper.class);
        job.setReducerClass(SortedFilePerMovieName.MovieSortReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);*/
        
        
       // 4th job      
        /*Job job = Job.getInstance(conf, "Movie Like Count");
        job.setJarByClass(Hproject.class);
        job.setMapperClass(MovieNameTotalUsers.TotalMovieMapper.class);
        job.setReducerClass(MovieNameTotalUsers.TotalMovieReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);*/
        
        //5th job 
     Job job = Job.getInstance(conf, "Movie Like Count movie list");
        job.setJarByClass(Hproject.class);
        job.setMapperClass(UserCountMoviesList.MovieLikeCountSortMapper.class);
        job.setReducerClass(UserCountMoviesList.MovieLikeCountSortReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
       
    }
    }

//hadoop jar /hadoop/labs/hproject/target/hproject-1.0-SNAPSHOT.jar com.mycompany.hproject.Hproject ratings.csv UserPreferredMovies 
//hadoop jar /hadoop/labs/hproject/target/hproject-1.0-SNAPSHOT.jar com.mycompany.hproject.Hproject  JoinOperation SortedFilePerMovieName
//hadoop jar /hadoop/labs/hproject/target/hproject-1.0-SNAPSHOT.jar com.mycompany.hproject.Hproject  JoinOperation MovieNameTotalUsers
//hadoop jar /hadoop/labs/hproject/target/hproject-1.0-SNAPSHOT.jar com.mycompany.hproject.Hproject  MovieNameTotalUsers UserCountMoviesList 

//hdfs dfs -copyFromLocal /hadoop/labs/hproject/target/JoinOperation /user/root/
//hdfs dfs -mkdir -p /user/root/UserPreferredMoviesWithName
//hadoop jar /hadoop/labs/hproject/target/hproject-1.0-SNAPSHOT.jar com.mycompany.hproject.Hproject UserPreferredMovies JoinOperation1 movies.csv
//hdfs dfs -rm -r /user/root/JoinOperation1
//hdfs dfs -copyToLocal /user/root/MovieNameTotalUsers /hadoop/labs
//hdfs dfs -ls hdfs://namenode:9000/user/root/movies.csv
//hdfs dfs -cat /user/root/movies.csv
//hdfs dfs -copyFromLocal /hadoop/labs/UserPreferredMovies /user/root/



/*hdfs dfs -copyFromLocal /hadoop/labs/JoinOperation /user/root/
hdfs dfs -copyFromLocal /hadoop/labs/hproject/target/ratings.csv /user/root/
hdfs dfs -copyFromLocal /hadoop/labs/UserPreferredMovies /user/root/*/

