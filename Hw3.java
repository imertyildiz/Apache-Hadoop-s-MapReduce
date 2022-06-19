import java.io.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;


public class Hw3 {

  private static class TotReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result_val = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int tot_sum = 0;
      for (IntWritable value : values) {
        tot_sum += value.get();
      }
      result_val.set(tot_sum);
      context.write(key, result_val);
    }
  }

  private static class TotMapper extends Mapper<Object, Text, Text, IntWritable>{
    private Text word_val = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] variables = value.toString().split("\t");
      if(variables[0].equals("artist")) return;
      Integer duration = Integer.parseInt(variables[2]);
      word_val.set("Total Count: ");
      context.write(word_val,new IntWritable(duration));
    }
  }

  private static class AvgReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {
    private DoubleWritable result_val = new DoubleWritable();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double tot_sum = 0;
      int count = 0 ;
      double avg = 0;
      for (IntWritable value : values) {
        tot_sum += (double)value.get();
        count+=1;
      }
      avg = tot_sum/count;
      result_val.set(avg);
      context.write(key, result_val);
    }
  }

  private static class AvgMapper extends Mapper<Object, Text, Text, IntWritable>{
    private Text word_val = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] variables = value.toString().split("\t");
      if(variables[0].equals("artist")) return;
      Integer duration = Integer.parseInt(variables[2]);
      word_val.set("Average Count: ");
      context.write(word_val,new IntWritable(duration));
    }
  }

  private static class PopReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result_val = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int tot_sum = 0;
      for (IntWritable value : values) {
        tot_sum += value.get();
      }
      result_val.set(tot_sum);
      context.write(key, result_val);
    }
  }

  private static class PopMapper extends Mapper<Object, Text, Text, IntWritable>{
    private Text word_val = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {


    String[] variables = value.toString().split("\t");
    if(variables[0].equals("artist"))
    {
      return;
    }
    Integer duration = Integer.parseInt(variables[2]);

      word_val.set(variables[0]);
      context.write(word_val,new IntWritable(1));
    }
  }

  private static class DanceMapper extends Mapper<Object, Text, Text, DoubleWritable>{
    private Text word_val = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] variables = value.toString().split("\t");
      if(variables[0].equals("artist")) {
        return;
      }
      Double dance_ability = Double.parseDouble(variables[6]);
      Integer year_val = Integer.parseInt(variables[4]);
      if (year_val <= 2002) {
        word_val.set("part-r-00000");
        context.write(word_val,new DoubleWritable(dance_ability));
      } else if (year_val <= 2012) {
        word_val.set("part-r-00001");
        context.write(word_val,new DoubleWritable(dance_ability));
      } else {
        word_val.set("part-r-00002");
        context.write(word_val,new DoubleWritable(dance_ability));
      }
    }
  }

  private static class DancePartitioner extends Partitioner<Text, DoubleWritable>{
    public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {
      if (numReduceTasks == 0)
        return 0;
      if (key.toString().equals("part-r-00000")) {
        return 0;
      } else if (key.toString().equals("part-r-00001")){
        return 1 % numReduceTasks;
      } else {
        return 2 % numReduceTasks;
      }
    }
  }

  private static class DanceReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result_val = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {
      double tot_sum = 0;
      double count = 0;
      for (DoubleWritable value : values) {
        tot_sum += value.get();
        count+=1;
      }
      result_val.set(tot_sum/count);
      context.write(key, result_val);
    }
  }

  private static class ExpPopMapper extends Mapper<Object, Text, Text, DoubleWritable>{
    private Text word_sum = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] variables = value.toString().split("\t");
      if(variables[0].equals("artist")) {
        return;
      }
      Integer popularity = Integer.parseInt(variables[5]);
      if (variables[3].toLowerCase().equals("true")) {
        word_sum.set("part-r-00000");
        context.write(word_sum,new DoubleWritable(popularity));
      } else {
        word_sum.set("part-r-00001");
        context.write(word_sum,new DoubleWritable(popularity));
      }
    }
  }

  private static class ExpPopPartitioner extends Partitioner<Text, DoubleWritable>{
      public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {
        if (numReduceTasks == 0)
          return 0;
        if (key.toString().equals("part-r-00000")) {
          return 0;
        }
        else {
          return 1 % numReduceTasks;
        }
      }
  }

  private static class ExpPopReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result_val = new DoubleWritable();
    public void reduce(Text key, Iterable<DoubleWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {
      double tot_sum = 0;
      double count = 0;
      double avg = 0;
      for (DoubleWritable value : values) {
        tot_sum += value.get();
        count+=1;
      }
      avg = tot_sum/count;
      result_val.set(avg);
      context.write(key, result_val);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "HW3");
    job.setJarByClass(Hw3.class);
    FileInputFormat.addInputPath(job,new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    if(args[0].equals("total")) {
      job.setMapperClass(TotMapper.class);
      job.setReducerClass(TotReducer.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
    }
    else if(args[0].equals("average")) {
      job.setMapperClass(AvgMapper.class);
      job.setReducerClass(AvgReducer.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
    }
    else if(args[0].equals("popular")) {
      job.setMapperClass(PopMapper.class);
      job.setReducerClass(PopReducer.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
    }
    else if (args[0].equals("explicitlypopular")) {
      job.setNumReduceTasks(2);
      job.setMapperClass(ExpPopMapper.class);
      job.setPartitionerClass(ExpPopPartitioner.class);
      job.setReducerClass(ExpPopReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
    }
    else if (args[0].equals("dancebyyear")) {
      job.setNumReduceTasks(3);
      job.setMapperClass(DanceMapper.class);
      job.setPartitionerClass(DancePartitioner.class);
      job.setReducerClass(DanceReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
    }
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
