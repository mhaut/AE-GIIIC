import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;


public class FlightsByCarrier {
	public static void main (String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(FlightsByCarrier.class); //Clase que va a ejecutar el trabajo
		job.setJobName("FlightsByCarrier"); //Identificador
		
		TextInputFormat.addInputPath(job, new Path(args[0])); //Fichero entrada
		TextOutputFormat.setOutputPath(job, new Path(args[1]));  //Fichero de salida
		
		job.setInputFormatClass(TextInputFormat.class); //Formato de entrada
		job.setMapperClass(FlightsByCarrierMapper.class);  //Clase para MAP
		job.setReducerClass(FlightsByCarrierReducer.class); //Clase para REDUCE
		
		job.setOutputFormatClass(TextOutputFormat.class); //Formato
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

//		job.addFileToClassPath(new Path("/user/root/opencsv-2.3.jar"));
		job.waitForCompletion(true); //Run&Wait

	}
}
