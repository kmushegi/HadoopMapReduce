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

	// public class TextTextInputFormat extends
 //    FileInputFormat<Text, Text> {

	//   public RecordReader<Text, Text> getRecordReader(
	//       InputSplit input, JobConf job, Reporter reporter)
	//       throws IOException {

	//     reporter.setStatus(input.toString());
	//     return new CombinerRecordReader(job, (FileSplit)input);
	//   }
	// }


	// private static final String OUTPUT_PATH = "merged_out";

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Error: Wrong number of parameters");
      		System.err.println("Expected: [impressions_merged] [clicks_merged] [combined] [ctr_out]");
      		System.exit(1);
		}
		jobDriver1(args[0], args[1], args[2]);
		//i think job2 should be taking args[2] as a parameter
		//in fact almost convinced its the issue with paths
    	return jobDriver2(args[2], args[3]);
	}

	public void jobDriver1(String impressions_merged, String clicks_merged, String combined) throws Exception{
		Configuration conf = getConf();

		Job job = new Job(conf,"Impressions Unifier");
		job.setJarByClass(ClickThru.class);

		job.setMapperClass(ClickThru.ImpressionsMapper.class);
		job.setReducerClass(ClickThru.ImpressionsReducer.class);

		Path[] inputPaths = new Path[2];
		inputPaths[0] = new Path(impressions_merged);
		inputPaths[1] = new Path(clicks_merged);
	    FileInputFormat.setInputPaths(job, inputPaths);
    	// FileInputFormat.addInputPath(job, new Path(inputPath));
    	FileOutputFormat.setOutputPath(job, new Path(combined));

    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);

    	job.waitForCompletion(true);
    	return;
	}

	public int jobDriver2(String combined, String ctr_out) throws Exception{
		Configuration conf = getConf();

		Job job = new Job(conf,"Impressions Unifier");
		job.setJarByClass(ClickThru.class);

		job.setMapperClass(ClickThru.ClicksMapper.class);
		job.setReducerClass(ClickThru.ClicksReducer.class);

		// FileInputFormat.setInputFormat
    	FileInputFormat.addInputPath(job, new Path(combined));
    	FileOutputFormat.setOutputPath(job, new Path(ctr_out));

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

			// StringBuilder parsedData = new StringBuilder();

			//take the substring of the val from the { to the end so that we get
			//a correct JSON String
			String valueString = val.toString();
			String jsnFormatString = valueString.substring(valueString.indexOf("{"));

			String impressionId = "";
				JSONObject jsnObj;
				try {
					System.out.println("Value of String is: "+jsnFormatString);
					jsnObj = new JSONObject(jsnFormatString);
					impressionId = (String)jsnObj.get("impressionId");
				} catch (JSONException e) {
					e.printStackTrace();
					throw new IOException("json exception: "+e);
				}
				outputKey.set(impressionId);
				if(jsnObj.has("referrer")) {
					try {
						String referrer = (String)jsnObj.get("referrer");
						System.out.println("referrer: " + referrer);
						String adId = (String)jsnObj.get("adId");
						System.out.println("adId: " + adId);
						// String val = (referrer+"\\x1f"+adId);
						// parsedData.append(referrer);
						// parsedData.append("\\x1f");
						// parsedData.append(adId);
						String oVStr = referrer+"\\x1f"+adId;
						outputValue.set(oVStr);
						System.out.println("Mapper Output - key:"+impressionId + ", val:" + oVStr);
					} catch (JSONException e) {
						e.printStackTrace();
					}	
				} else {
					outputValue.set("1");
					System.out.println("Mapper Output - key:"+impressionId + ", val:1");
				}
				context.write(outputKey,outputValue);
		}
	}
	
	//add values of [impression ID, url, adID]: 0 +1 = 1 if clicked through, or just 0 if only impression
	public static class ImpressionsReducer extends Reducer<Text,Text,LongWritable,Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
							throws IOException, InterruptedException {

				
				int impressionsTotal = 0;
				String newKeyString = "";
				for(Text value : values) {
					String valueStr = value.toString();
					System.out.println("valueStr: "+ valueStr);
					if(value.toString().contains("\\x1f")){
						newKeyString = valueStr;
					} else {
						impressionsTotal = 1;
					}
				}
				String keyWithValueString = (newKeyString+"\\x1e"+String.valueOf(impressionsTotal));
				// String newKeyString = (url+"\\x1f"+adId);
				// String val = String.valueOf(impressionsTotal);
				// Text newKey = new Text(newKeyString);
				// Text outputValue = new Text(val);
				context.write(new LongWritable(0),new Text(keyWithValueString));

				// context.write(new Text(newKeyString),new Text(String.valueOf(impressionsTotal)));
		}
	}
	//OLD vvvv
	// INPUT: [url, adID] -> 0 or 1 
	// OUTPUT: [url, adID] -> 0 or 1

	//NEW vvvv
	// INPUT: [#] -> url\x1fadID\x1e0 or 1
	// OUTPUT: [url, adID] -> 0 or 1
	public static class ClicksMapper extends Mapper<LongWritable,Text,Text,Text> {

		// private Text outputKey2 = new Text();
		// private Text outputValue2 = new Text();

		@Override
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
			// String tempString = key.toString();
			// Text newKey = new Text(tempString);
			// String valueStr = val.toString();

			System.out.println("val: " + val.toString());
			String[] key_val = val.toString().split("(\\x1f)|(\\\x1e)");
			System.out.printf("key_val length: %d", key_val.length);
			//should produce array 0 - referrer, 1 - adId, 2 - click/imp bool

			// String[] doubleKey = key_val[0].split("\\x1f");
			// String key = (doubleKey[0] + ", " + doubleKey[1]);
			// String key = key_val[0].replaceAll("\\x1f", ", ");
			// String keyF = key_val[0].replaceAll("\\x1f", ", ");
			// String valF = key_val[1];
			String outputKey = key_val[0] + key_val[1];
			String outputVal = key_val[2];
			context.write(new Text(outputKey),new Text(outputVal));

			// if(key_val.length==3){
			// 	context.write(new Text(key_val[0].replaceAll("\\x1f", ", ")),new Text(key_val[1]));
			// }
			
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