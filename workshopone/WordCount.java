package workshopone;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount
{

    public static void main(String[] args) throws Exception
    {

        // Validate inputs
        if (args.length !=2) 
        {
            System.exit(-1);
        }
        Job job = new Job();

        // Define what jar to use to run
        job.setJarByClass(WordCount.class);
        
        // Set job name
        job.setJobName("Word Count");
        // Set file paths based on input args
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set which class does which part
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(SumReducer.class);


        // Set data types of output keys and values
        job.setOutputKeyClass(Text.class); // String
        job.setOutputValueClass(IntWritable.class); // Integer
        
        // Finish if finished
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);

    }
}
