

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Solution2 
{
		
		public static class UserMapper extends Mapper<LongWritable, Text, Text, Text>
		{
			
			private HashMap<String, String> userIdToInfo = new HashMap<String, String>();
			
			@Override
			protected void setup(Context context)throws IOException, InterruptedException
			{
				// TODO Auto-generated method stub
				super.setup(context);
	
				@SuppressWarnings("deprecation")
				Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				
				for(Path myFile: files)
				{
					String nameofFile=myFile.getName();
					File file =new File(nameofFile+"");
					FileReader fr= new FileReader(file);
					BufferedReader br= new BufferedReader(fr);
					//BufferedReader bufferedReader = new BufferedReader(new FileReader(p.toString()));
					String line;
					while ((line = br.readLine()) != null) {
						String[] splitArray = line.split("::");
						if(splitArray != null && !splitArray[0].isEmpty()) {
							userIdToInfo.put(splitArray[0].trim(),line);
						}
					}
					br.close();
				}
				
			}
	
	
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
			{
				String[] ratingArray = value.toString().split("::");
				
				Configuration conf = context.getConfiguration();
				String inMovieId = conf.get("movieId").toString();
				if(ratingArray != null && !ratingArray[0].isEmpty())
				{
					String userInformation = userIdToInfo.get(ratingArray[0]);
					String[] userINformationArray = userInformation.split("::");
					if(userInformation != null) 
					{
						if(inMovieId.equals(ratingArray[1]))
						{
							int rating = Integer.parseInt(ratingArray[2]);
							if(rating>=4)
							{
								String userId = userINformationArray[0];
								String gender = userINformationArray[1];
								String age = userINformationArray[2];
								context.write(new Text(""), new Text(userId + " " + gender + " " + age));
							}
								
						}
					}
				}
			}
	
		}


	
	
		//The reducer class	
		public static class Reduce extends Reducer<Text,Text,Text,Text> 
		{
			private Text result = new Text();
			private Text myKey = new Text();
			//note you can create a list here to store the values

			public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException 
			{
				for (Text val : values)
				{
					result.set(val.toString());
					myKey.set(key.toString());
					
					context.write(myKey,result );
					
				}
			}		
		}
	
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// get all args
		if (otherArgs.length != 4)
		{
			System.err.println("Usage: JoinExample <in> <in2> <out> <anymovieid>");
			System.exit(2);
		}
		
		conf.set("movieId", otherArgs[3]);
		DistributedCache.addCacheFile(new URI(args[0]), conf);
		//final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		
		// File to be stored in map-side cache
		
		//
		
		Job job = Job.getInstance(conf, "listusersmapside"); 
		job.setJarByClass(Solution2.class);
		//job.addCacheFile(new URI(args[0]));
		job.setReducerClass(Reduce.class);
		
		//job.addCacheFile(new URI(NAME_NODE+ "/user/hue/users/users.dat"));
		job.setMapperClass(UserMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//set the HDFS path of the input data
		// set the HDFS path for the output 
		FileInputFormat.addInputPath(job,new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.waitForCompletion(true);
	}

}