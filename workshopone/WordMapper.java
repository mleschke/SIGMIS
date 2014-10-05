package workshopone;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{

    @Override
    // Context allows communication with hadoop
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {
        // Take input 
        String line = value.toString();

        // Split word on non-word characters
        for (String word : line.split("\\W+")) 
        {
            if (word.length() > 0)
            {
                // Write key value pair of correct data type
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

}
