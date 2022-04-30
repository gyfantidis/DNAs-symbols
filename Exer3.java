package exer3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class Exer3 {
    /**
     * The mapper 
     */
    public static class DnaMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text ecoli = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {


            String str = value.toString();
            String[] lines = str.split(System.lineSeparator());

            for(int n=0; n<lines.length; n++) {
                char[] ch = lines[n].toCharArray();

                for (int i = 2; i <= 4; i++) {
                    String dna = "";
                    for (int j = 0; j <= (ch.length - i); j++) {
                        for (int k = 0; k < i; k++) {
                            dna = (dna + "" + ch[j + k]);
                        }
                        ecoli.set(dna);
                        context.write(ecoli, one);
                        dna = "";
                    }

                }
            }
            }
        }


    /**
     * This is the classic summary reducer which is used in the word count example
     */
    public static class DnaReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {




            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }

                result.set(sum);
                context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Exercise3DNA ");
        job.setJarByClass(Exer3.class);
        job.setMapperClass(Exer3.DnaMapper.class);
        job.setReducerClass(Exer3.DnaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
