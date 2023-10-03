// import java.io.IOException;
// import au.com.bytecode.opencsv.CSVParser;
// import org.apache.hadoop.io.*;
// import org.apache.hadoop.mapreduce.Mapper;
//
// //Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
// public class FlightsByCarrierMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
// 	@Override
// 	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
// 		if (key.get() > 0) { //Ignora la primera linea
// 			String[] lines = new CSVParser().parseLine(value.toString());
// 			context.write(new Text(lines[8]), new IntWritable(1));
// 		}
// 	}
// }
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightsByCarrierMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() > 0) { // Ignora la primera línea
            String[] fields = value.toString().split(",");
            if (fields.length >= 9) {
                Text carrier = new Text(fields[8]); // Tomar el campo en la posición 8 como clave
                IntWritable one = new IntWritable(1);
                context.write(carrier, one);
            }
        }
    }
}
