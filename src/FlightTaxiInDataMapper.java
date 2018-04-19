import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightTaxiInDataMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineSplit = line.split(",");
		// TaxiIn
		if (!lineSplit[8].equals("NA") && !lineSplit[19].equals("NA") && lineSplit[19] != null
				&& !lineSplit[8].equals("UniqueCarrier") && !lineSplit[19].equals("TaxiIn")) {
			context.write(new Text(lineSplit[8]), new LongWritable(Long.parseLong(lineSplit[19])));
		}

	}

}
