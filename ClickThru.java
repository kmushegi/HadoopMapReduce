import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.json.simple.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.lang.*;

public class ClickThru extends Configured implements Tool {

	public static void main(String[]  args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ClickThru(), args);
		System.exit(res);
	}
	private static final String OUTPUT_PATH = "merged_out";

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Error: Wrong number of parameters");
      		System.err.println("Expected: [impressions_merged] [clicks_merged] [out]");
      		System.exit(1);
		}

		jobDriver1(args[0]);
    	return jobDriver2(args[1]);
	}

	public void jobDriver1(String inputPath) throws Exception{
		Configuration conf = getConf();

		Job job = new Job(conf,"Impressions Unifier");
		job.setJarByClass(ClickThru.class);

		job.setMapperClass(ClickThru.ImpressionsMapper.class);
		job.setReducerClass(ClickThru.ImpressionsReducer.class);

    	FileInputFormat.setInputPath(job, new Path(inputPath));
    	FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

    	job.setOutputKeyClass(Text.class);
    	job.setOutputValeClass(Text.class);

    	job.waitForCompletion(true);
    	return;

	}

	public int jobDriver2(String outputPath) throws Exception{
		Configuration conf = getConf();

		Job job = new Job(conf,"Impressions Unifier");
		job.setJarByClass(ClickThru.class);

		job.setMapperClass(ClickThru.ClicksMapper.class);
		job.setReducerClass(ClickThru.ClicksReducer.class);

    	FileInputFormat.setInputPath(job, new Path(OUTPUT_PATH));
    	FileOutputFormat.setOutputPath(job, new Path(outputPath));

    	job.setOutputKeyClass(Text.class);
    	job.setOutputValeClass(Text.class);

    	return job.waitForCompletion(true) ? 0 : 1;

	}
	//map [impression ID, url, adID] key to either 0 (impressions collection) or 1 (clicks collection) val
	public static class ImpressionsMapper extends Mapper<LongWritable,Text,Text,Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void map(LongWritable key, Text val, Context context) 
							throws IOException, InterruptedException {

			JSONParser parser = new JSONParser();
			StringBuilder parsedData = new StringBuilder();

			String impressionId;
			String referrer;
			String adId;
			String behavior;

			try {
				JSONObject jsnObj = new JSONObject(val.toString());
				impressionsId = (String)jsnObj.get("impressionId");
				outputKey.set(impressionId);
				if(jsnObj.containsKey("referrer")) {
					try {
						referrer = (String)jsnObj.get("referrer");
						adId = (String).jsnObj.get("adId");
						//behavior = "0";
						parsedData.append(referrer);
						parsedData.append("\\x1f");
						parsedData.append(adId);
						//parsedData.append("\x1f");
						//parsedData.append(behavior);
						outputValue.set(parsedData.toString());
						context.write(outputKey,outputValue);
					} catch (JSONException e) {
						e.printStackTrace();
					}
				} else {
					behavior = "1";
					outputValue.set(behavior);
					context.write(outputKey,outputValue);
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}
	
	//add values of [impression ID, url, adID]: 0 +1 = 1 if clicked through, or just 0 if only impression
	public static class ImpressionsReducer extends Reducer<Text,Text,Text,Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
							throws IOException, InterruptedException {

				
				int impressionsTotal = 0;
				String url;
				String adId;
				for(Text value : values) {
					String splitInput[] = value.toString().split("(\\x1f)");
					if(splitInput.length() == 1) {
						impressionsTotal = 1;
					} else {
						url = splitInput[0];
						adId = splitInput[1];
					}
				}
				String newKeyString = (url+"\\x1f"+adId);
				String val = Integer.toString(impressionsTotal);
				Text newKey = new Text(newKeyString);
				Text outputValue = new Text(val);
				context.write(newKey,outputValue);
		}

	}
	//INPUT: [url, adID] -> 0 or 1
	//OUTPUT: [url, adID] -> 0 or 1
	public static class ClicksMapper extends Mapper<LongWritable,Text,Text,Text> {

		@Override
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
			context.write(key,val);
		}

	}

	//INPUT: [url, adID] -> vals[0,1,1...]
	//OUTPUT: [url, adID] -> 1 count / totalCount = CTR
	public static class ClicksReducer extends Reducer<Text,Text,Text,Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
							throws IOException, InterruptedException {
				int totalImpressions = 0;
				int totalClicks = 0;
				for(Text value : values){
					totalImpressions++;
					totalClicks += Integer.parseInt(value.toString());
				}
				Text clickThroughRate = new Text();
	       		clickThroughRate.set(Strings.Itoa(totalClicks/totalImpressions));
	        	context.write(key, clickThroughRate);
		}

	}

}