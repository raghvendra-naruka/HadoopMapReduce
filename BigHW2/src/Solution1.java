
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Solution1 {
	
	public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
	   
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String line = value.toString();
	    	String []splitArray = line.split("::");
	    	if(splitArray != null && !splitArray[0].isEmpty() && !splitArray[1].isEmpty()) {
	    		if(splitArray[1].trim().equals("F")) {
		            String userId = splitArray[0].trim();
		            context.write(new Text(userId), new Text("U" + userId));
	    		}
	    	}
	    }
	}   
	 
	
	 public static class RatingsMapper extends Mapper<LongWritable, Text, Text, Text> {

		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    	String line = value.toString();
		    	String []splitArray = line.split("::");
		    	if(splitArray != null && !splitArray[0].isEmpty() && !splitArray[1].isEmpty() && !splitArray[2].isEmpty()) {
		            String userId = splitArray[0].trim();
		            String movieId = splitArray[1].trim();
		            String rating = splitArray[2].trim();
		            String concatMovieIdAndRating = movieId + "~" + rating;
		            context.write(new Text(userId), new Text("R" + concatMovieIdAndRating));
	    	     }
		 
		    }
	 }
	 
	 public static class UserRatingsReducer extends Reducer<Text, Text, Text, Text> {
		 
		 private Text tmp = new Text();
		 private ArrayList<Text> listA = new ArrayList<Text>();
		 private ArrayList<Text> listB = new ArrayList<Text>();
		 public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	    	listA.clear();
	    	listB.clear();
	    	for(Text val : values) {
	    		tmp = val;
	    		if (tmp.charAt(0) == 'U') {
	        		listA.add(new Text(tmp.toString().substring(1)));
	    		}
	    		else if (tmp.charAt(0) == 'R') {
	        		listB.add(new Text(tmp.toString().substring(1)));
	    		}
	    	}
	    	executeJoinLogic(context);
	    	}
		 private void executeJoinLogic(Context context) throws IOException, InterruptedException {
			 if (!listA.isEmpty() && !listB.isEmpty()) {
				 for (Text A : listA) {
					 for (Text B : listB) {
						 context.write(new Text(B.toString().split("~")[0]), new Text(B.toString().split("~")[1]));
					 }
					 }
				 }
			 }
		 }
	 
	 public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    	String line = value.toString();
		    	String []splitArray = line.split("::");
		    
		        String movieId = splitArray[0].trim();
		        String movieName = splitArray[1].trim();
		        String movieGenre = splitArray[2].trim();
		        String concatMovieNameAndGenre = movieName + "~" + movieGenre;
		        context.write(new Text(movieId), new Text("$"+concatMovieNameAndGenre));
		    	
		    }
		 }
	 public static class UserRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    	String line = value.toString();
		    	String []splitArray = line.split("\t");
		    	if(splitArray != null && !splitArray[0].isEmpty() && !splitArray[1].isEmpty()) {
		    		 String movieId = splitArray[0].trim();
		    		 String movieRating = splitArray[1].trim();
			         context.write(new Text(movieId), new Text("#"+movieRating));
			         }
		    	}
		    }
	 public static class UserMoviesRatingsReducer extends Reducer<Text, Text, Text, Text> {
		 private ArrayList<Text> listA = new ArrayList<Text>();
		 private ArrayList<Text> listB = new ArrayList<Text>();
		 public void reduce(Text key, Iterable<Text> values, Context context)
				 throws IOException, InterruptedException {
		    	listA.clear();
		    	listB.clear();
		 double sum = 0;
	     double count = 0;
	     for (Text val : values) {
	    	 if (val.charAt(0) == '#') {
	    		 sum += Double.parseDouble(val.toString().substring(1));
	             count += 1;
	 		}
	    	 
	    	 else if(val.charAt(0) == '$') {
	    		 listB.add(new Text(key.toString() + "~" + val.toString().substring(1)));
	    	 }
	     }
	     //String averageRating = Double.toString(sum / count);
	     double averageRating = sum / count;
	     averageRating = Math.round(averageRating * 100);
	     averageRating = averageRating/100;

	     listA.add(new Text(key.toString() + "~" + Double.toString(averageRating)));
	     executeSecondJoinLogic(context);
	 }
		 
   private void executeSecondJoinLogic(Context context) throws IOException, InterruptedException {
			 if (!listA.isEmpty() && !listB.isEmpty()) {
				 for (Text A : listA) {
					 for (Text B : listB) {
						 context.write(new Text(B.toString().split("~")[1]), 
								 new Text(A.toString().split("~")[1]));
					 }
					}
				 }
			 }
	 }
	 
	 public static class TopFiveMoviesMapper extends Mapper<LongWritable, Text, Text, Text> {
		 private Text singleKey=new Text("singleKey");
		 private Text titlerating = new Text();
		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    	String line = value.toString();
		    	String []splitArray = line.split("\t");
		    	if(splitArray != null && !splitArray[0].isEmpty() && !splitArray[1].isEmpty()) {
		    		titlerating.set(splitArray[0].trim()+"~"+splitArray[1].trim());
			         context.write(singleKey,titlerating);
			         }
		    	}
		    }
	 public static class RatingComparator implements Comparator<Movie> {
	
		 public int compare(Movie keyA, Movie keyB) {
		 Double valueA = (Double)keyA.rating;
		 Double valueB = (Double) keyB.rating;
		 if (valueA == valueB || valueB > valueA) {
		 return 1;
		 } else {
		 return -1;
		 }
		 }
		 }
	 
	 public static class TopFiveMoviesReducer extends Reducer<Text, Text, Text, Text> {
		 
		private Comparator<Movie> comparator = new RatingComparator(); 
		private PriorityQueue<Movie> pq=new PriorityQueue<Movie>(5,comparator);
		private Movie m;
		private Text movieTitle=new Text();
		private Text movieRating=new Text();
		 public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
			 
			 for (Text val : values) {
				 	String[] split=val.toString().split("~");
				 	m=new Movie();
				 	m.movieTitle=split[0].trim();
				 	m.rating=Double.parseDouble(split[1].trim());
				 	pq.add(m);
		     }
			 
			 movieTitle.set("Movie Title");
			 movieRating.set("Average Rating By Female Users");
			 context.write(movieTitle, movieRating);
			 int i=0;
			 while(i<5){
				 m=pq.poll();
				 movieTitle.set(m.movieTitle);
				 movieRating.set(Double.toString(m.rating));
				 context.write(movieTitle, movieRating);
				 i++;
			 }
			 
			 			
			}
		}
	 
	 public static class Movie{
		 
		 String movieTitle;
		 double rating;
		 
	 }
	 public static void main(String[] args) throws Exception {
		 
		    Configuration conf = new Configuration();        
		    Job job = Job.getInstance(conf, "Solution1");
			job.setJarByClass(Solution1.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,UserMapper.class);
		    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingsMapper.class);
		    job.setReducerClass(UserRatingsReducer.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    FileOutputFormat.setOutputPath(job, new Path(args[3]));
		    job.waitForCompletion(true);
		    Job job2=null;
		    if(job.isSuccessful())
		    {
		    	Configuration conf2 = new Configuration();        
		    	job2 = Job.getInstance(conf2, "Solution2");
		        job2.setJarByClass(Solution1.class);
		        job2.setOutputKeyClass(Text.class);
		        job2.setOutputValueClass(Text.class);
		        job2.setInputFormatClass(TextInputFormat.class);
		        job2.setOutputFormatClass(TextOutputFormat.class);
		        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, MovieMapper.class);
		        MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, UserRatingMapper.class);
		        job2.setReducerClass(UserMoviesRatingsReducer.class);
		        job2.setOutputFormatClass(TextOutputFormat.class);
		        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
		        job2.waitForCompletion(true);
		    }
		    if(job2!=null && job2.isSuccessful())
		    {
		    	Configuration conf3 = new Configuration();        
		    	Job job3 = Job.getInstance(conf3, "Solution3");
		    	job3.setJarByClass(Solution1.class);
		    	job3.setOutputKeyClass(Text.class);
		    	job3.setOutputValueClass(Text.class);
		    	job3.setInputFormatClass(TextInputFormat.class);
		    	job3.setOutputFormatClass(TextOutputFormat.class);
		        MultipleInputs.addInputPath(job3, new Path(args[4]), TextInputFormat.class, TopFiveMoviesMapper.class);
		        job3.setReducerClass(TopFiveMoviesReducer.class);
		        job3.setOutputFormatClass(TextOutputFormat.class);
		        FileOutputFormat.setOutputPath(job3, new Path(args[5]));
		        job3.waitForCompletion(true);
		    }
		    
		 }
	 
	
}
