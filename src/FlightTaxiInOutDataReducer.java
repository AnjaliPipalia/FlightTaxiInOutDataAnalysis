import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightTaxiInOutDataReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {
	public TreeSet<FlightWithHighestAverage> top3 = new TreeSet<>();
	public TreeSet<FlightWithLowestAverage> bottom3 = new TreeSet<>();

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0;
		long numberOfFlights = 0;
		for (LongWritable val : values) {
			sum += val.get();
			numberOfFlights++;

		}
		double average =  (sum) / (double) (numberOfFlights);
		top3.add(new FlightWithHighestAverage(key.toString(), average));
		if (top3.size() > 3) {
			top3.pollLast();
		}
		bottom3.add(new FlightWithLowestAverage(key.toString(), average));

		if (bottom3.size() > 3) {
			bottom3.pollLast();
		}

	}

	@Override
	protected void cleanup(Reducer<Text, LongWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(new Text("highest average"), null);
		for (FlightWithHighestAverage key : top3) {
			context.write(new Text(key.uniqueID), new DoubleWritable(key.average));
		}
		context.write(new Text("lowest average"), null);
		for (FlightWithLowestAverage key : bottom3) {
			context.write(new Text(key.uniqueID), new DoubleWritable(key.average));
		}
	}

	public class FlightWithHighestAverage implements Comparable<FlightWithHighestAverage> {
		double average;
		String uniqueID;

		FlightWithHighestAverage(String string, double probability) {
			this.average = probability;
			this.uniqueID = string;
		}

		@Override
		public int compareTo(FlightWithHighestAverage flightWithHighestAverage) {

			if (this.average < flightWithHighestAverage.average) {
				return 1;
			} else if (this.average == flightWithHighestAverage.average) {
				return 0;
			} else
				return -1;

		}
	}

	public class FlightWithLowestAverage implements Comparable<FlightWithLowestAverage> {
		double average;
		String uniqueID;

		FlightWithLowestAverage(String string, double average) {
			this.average = average;
			this.uniqueID = string;
		}

		@Override
		public int compareTo(FlightWithLowestAverage flightWithLowestAverage) {

			if (this.average > flightWithLowestAverage.average) {
				return 1;
			} else if (this.average == flightWithLowestAverage.average) {
				return 0;
			} else
				return -1;

		}
	}

}
