# MovieAnalytics
<p align="center">
  <a href="https://www.docker.com/"><img src="https://www.vectorlogo.zone/logos/docker/docker-icon.svg" alt="Docker" height="70"></a>
  <a href="https://hadoop.apache.org/"><img src="https://www.vectorlogo.zone/logos/apache_hadoop/apache_hadoop-icon.svg" alt="Hadoop" height="70"></a>
  <a href="https://www.java.com/"><img src="https://www.vectorlogo.zone/logos/java/java-horizontal.svg" alt="Java" height="70"></a>
  <a href="https://maven.apache.org/"><img src="https://www.vectorlogo.zone/logos/apache_maven/apache_maven-icon.svg" alt="Maven" height="70"></a>
</p>


## Overview
MovieAnalytics is a data analysis project leveraging the Hadoop framework to provide insights into user preferences and trends in movie ratings. By utilizing big data technologies, this project enables efficient processing of large datasets containing movie ratings, user information, and other relevant data points.

## Objectives
The primary goals of MovieAnalytics are to:
- Analyze user ratings and preferences for various movies.
- Identify trends and patterns in movie consumption.
- Generate reports that summarize user engagement and movie popularity.

## Key Features
- **Data Processing:** Utilizes Hadoop's MapReduce framework to handle large volumes of movie and user data efficiently.
- **Join Operations:** Implements join operations to combine multiple datasets for comprehensive analysis.
- **User Preference Analysis:** Determines users' preferred movies based on their ratings and interactions.
- **Reporting:** Generates sorted lists of movies by user count and overall ratings, providing valuable insights into user behavior and movie performance.

## Technologies Used
- **Hadoop:** For distributed storage and processing of big data.
- **Java:** For writing MapReduce jobs and data processing algorithms.
- **Docker:** To create isolated environments for running Hadoop clusters.

## Getting Started

### Prerequisites
- Docker installed on your machine.
- Basic understanding of Hadoop and MapReduce concepts.

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/khouloudcherrat/MovieAnalytics.git
   ```
2. Navigate to the project directory:
   ```bash
   cd MovieAnalytics
   ```
3. Build the Docker container for Hadoop:
   ```bash
   docker-compose up -d
   ```

### Running the Analysis
1. Load the datasets into HDFS (Hadoop Distributed File System):
   ```bash
   hdfs dfs -put /path/to/local/movie_data.csv /input/
   hdfs dfs -put /path/to/local/user_data.csv /input/
   ```
2. Execute the MapReduce job:
   ```bash
   hadoop jar MovieAnalytics.jar com.example.MovieAnalyticsMain
   ```
3. Retrieve the output from HDFS:
   ```bash
   hdfs dfs -get /output/ /path/to/local/output/
   ```

## Use Cases
- **Movie Recommendations:** Provide personalized movie recommendations to users based on their preferences.
- **Market Analysis:** Assist film studios and distributors in understanding audience preferences and trends.
- **User Engagement Reports:** Generate reports for marketers and content creators to tailor their strategies based on user engagement data.

## Contributing
Contributions are welcome! Please fork the repository and submit a pull request for any enhancements or fixes.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments
- Special thanks to the contributors and the open-source community for their support and resources.

