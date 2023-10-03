import java.io.IOException;
//import au.com.bytecode.opencsv.CSVParser;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
public class FlightsByCarrierMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() > 0) { //Ignora la primera linea
//			String[] lines = new CSVParser().parseLine(value.toString());
			List<String[]> lines = parseCSV(csvData);
			context.write(new Text(lines[8]), new IntWritable(1));
		}
	}

	public static List<String[]> parseCSV(String csvData) throws IOException {
	List<String[]> records = new ArrayList<>();
	try (BufferedReader reader = new BufferedReader(new StringReader(csvData))) {
		String line;
		while ((line = reader.readLine()) != null) {
			String[] fields = line.split(",");
			records.add(fields);
		}
	}
	return records;
    }
}
