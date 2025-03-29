import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FrequencyAnalysis {

    // Mapper Class: Tokenizes the job title into individual words
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Skip the header row
            if (isHeader && line.toLowerCase().contains("title")) {
                isHeader = false;
                return;
            }
            isHeader = false;

            // Split the CSV line by comma; adjust if your CSV contains commas in fields
            String[] fields = line.split(",");
            if (fields.length > 0) {
                // Assuming the first column is the job title
                String title = fields[0].trim();
                // Tokenize the job title: split on non-word characters
                String[] tokens = title.split("\\W+");
                for (String token : tokens) {
                    if (!token.isEmpty()) {
                        word.set(token.toLowerCase());
                        context.write(word, one);
                    }
                }
            }
        }
    }

    // Reducer Class: Sums up the counts for each word
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Driver Code: Sets up and runs the job
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: FrequencyAnalysis <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Frequency Analysis");
        job.setJarByClass(FrequencyAnalysis.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
