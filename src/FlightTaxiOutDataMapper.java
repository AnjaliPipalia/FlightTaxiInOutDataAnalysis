import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightTaxiOutDataMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineSplit = line.split(",");
		//TaxiOut
		if (!lineSplit[8].equals("NA") &&!lineSplit[20].equals("NA") &&  lineSplit[20] !=null && !lineSplit[8].equals("UniqueCarrier")
				&& !lineSplit[20].equals("TaxiOut")) {
			context.write(new Text(lineSplit[8]), new LongWritable(Long.parseLong(lineSplit[20])));
		}
		
	}

}
