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
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;
import java.lang.*;

public class ClickThru extends Configured implements Tool {

	public static void main(String[]  args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ClickThru(), args);
		System.exit(res);
	}
	//this is causing an issue, because if the path (directory) already
	//exists Hadoop will not let you run anything. need a work around
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

    	FileInputFormat.addInputPath(job, new Path(inputPath));
    	FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);

    	job.waitForCompletion(true);
    	return;
	}

	public int jobDriver2(String outputPath) throws Exception{
		Configuration conf = getConf();

		Job job = new Job(conf,"Impressions Unifier");
		job.setJarByClass(ClickThru.class);

		job.setMapperClass(ClickThru.ClicksMapper.class);
		job.setReducerClass(ClickThru.ClicksReducer.class);

    	FileInputFormat.addInputPath(job, new Path(OUTPUT_PATH));
    	FileOutputFormat.setOutputPath(job, new Path(outputPath));

    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);

    	return job.waitForCompletion(true) ? 0 : 1;

	}
	//map [impression ID, url, adID] key to either 0 (impressions collection) or 1 (clicks collection) val
	public static class ImpressionsMapper extends Mapper<LongWritable,Text,Text,Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void map(LongWritable key, Text val, Context context) 
							throws IOException, InterruptedException {

			StringBuilder parsedData = new StringBuilder();

			String impressionId = null;
			String referrer;
			String adId;
			String behavior;
				JSONObject jsnObj;
				try {
					System.out.println("Value of String is: "+val.toString());
					jsnObj = new JSONObject(val.toString());
					impressionId = (String)jsnObj.get("impressionId");
				} catch (JSONException e) {
					e.printStackTrace();
					throw new IOException("json exception: "+e);
				}
				outputKey.set(impressionId);
				if(jsnObj.has("referrer")) {
					try {
						referrer = (String)jsnObj.get("referrer");
						adId = (String)jsnObj.get("adId");
						//behavior = "0";
						parsedData.append(referrer);
						parsedData.append("\\x1f");
						parsedData.append(adId);
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
		}
	}
	
	//add values of [impression ID, url, adID]: 0 +1 = 1 if clicked through, or just 0 if only impression
	public static class ImpressionsReducer extends Reducer<Text,Text,Text,Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
							throws IOException, InterruptedException {

				
				int impressionsTotal = 0;
				String url = null;
				String adId =null;
				for(Text value : values) {
					String splitInput[] = value.toString().split("(\\x1f)");
					if(splitInput.length == 1) {
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
			String tempString = key.toString();
			Text newKey = new Text(tempString);
			context.write(newKey,val);
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
	       		clickThroughRate.set(Integer.toString(totalClicks/totalImpressions));
	        	context.write(key, clickThroughRate);
		}
	}
}