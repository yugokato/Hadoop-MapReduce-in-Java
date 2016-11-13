package homework;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Calendar;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MR2Screener1 {
	public static class myReducer extends Reducer<Text, Text, Text, Text> {
		static NumberFormat formatter = new DecimalFormat("#0.00");

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> symbolList = new ArrayList<>();
			String[] line;
			int symbolCount = 0;
			String symbol = "";
			double totalMarketCap = 0;
			String marketCapStr;
			String outValue;

			for (Text value : values) {
				line = value.toString().split(",");
				symbol = line[0];
				marketCapStr = line[1];
				if (marketCapStr.equalsIgnoreCase("n/a")) {
					marketCapStr = "0";
				}
				totalMarketCap += Double.parseDouble(marketCapStr);
				symbolList.add(symbol);
				symbolCount++;
			}
			outValue = symbolList.toString() + ", " + symbolCount + ", " + formatter.format(totalMarketCap);
			// Output format: key: Sector, value: [Symbols], num_of_symbols,
			// total_market_cap
			context.write(new Text(key), new Text(outValue));
		}

	}

	public static class myMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			double marketCapBillion;
			String outKey;
			String outValue;
			String delimiter = "\",\"";
			String[] line = value.toString().split(delimiter);
			if (line.length != 8) {
				System.out.println("SKIPPED THE INVALID DATA");
			}

			else {
				boolean isMillion = false;
				String marketCapStr = line[3].trim();
				double marketCap = 0;
				String sector = line[5].trim();
				String symbol = line[0].replaceAll("\"", "").trim();

				if (marketCapStr.matches(".*B$") || symbol.equals("Symbol")) {
					// Skip the header entry and entries which have >= 1B market
					// cap
				}

				else {
					if (marketCapStr.matches(".*M$")) {
						isMillion = true;
					}

					marketCapStr = marketCapStr.replaceAll("[$ | M ]", "");

					if (isMillion == true) {
						marketCap = Double.parseDouble(marketCapStr) * 100000000;
					}
					outKey = sector;
					outValue = symbol + "," + Double.toString(marketCap);

					context.write(new Text(outKey), new Text(outValue));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: Provide <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(MR2Screener1.class);
		job.setJobName("Stock Screener");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		String output = args[1] + "_" + Calendar.getInstance().getTimeInMillis();
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(myMapper.class);
		job.setReducerClass(myReducer.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
