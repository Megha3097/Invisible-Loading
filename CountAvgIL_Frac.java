import java.util.*;
import java.io.*;

import java.io.IOException; 
import java.io.IOException; 

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.util.*; 

public class CountAvgIL_Frac 
{
   //Mapper class 
   public static class E_EMapper extends MapReduceBase implements 
   Mapper<LongWritable ,/*Input key Type */ 
   Text,                /*Input value Type*/ 
   Text,                /*Output key Type*/ 
   IntWritable>        /*Output value Type*/ 
   {
      Integer col_id, oset, lrange, hrange;

      // configure
      public void configure(JobConf job){
         super.configure(job);

         col_id = job.getInt("col_id", 1);
         oset = job.getInt("oset", 1);

         lrange = (oset-1)*2000+1;
         hrange = oset*2000;
      }


      //Map function 
      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 

         String line = value.toString();
         StringTokenizer s = new StringTokenizer(line,"\t");
         String cell = null;
         int id=500;

         for(int i=0;i<=5;i++){
            cell = s.nextToken();

            if(i==0){
               id = Integer.parseInt(cell);
            }

            if(i==col_id && lrange <= id && id <= hrange){
               int val = Integer.parseInt(cell);

               // q3 - update table main_table set col_id=val where ID=id
               String exec_cmd2 = "python3 ../monet/python_files/q3_frac.py "+col_id+" "+val+" "+id;
               Process p = Runtime.getRuntime().exec(exec_cmd2);
               
               output.collect(new Text("col"+col_id), new IntWritable(val));
            }
         }
      } 
   }
   
   //Reducer class 
   public static class E_EReduce extends MapReduceBase implements Reducer< Text, IntWritable, Text, IntWritable > {

      //Reduce function 
      public void reduce( Text key, Iterator <IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 

         long n = 0;
         long sum = 0;
         while(values.hasNext()){
            n++;
            sum+=values.next().get();
         }

         int avg = (int)(sum / n);
         output.collect(new Text("average"), new IntWritable(avg));
      }
   }

   //Main function 
   public static void main(String args[])throws Exception { 
      long stime = System.currentTimeMillis();

      String col_id = args[2];
      String oset = args[3];

      // q1 - select col_id from catalog;
      // q1_frac - select loaded from catalog where col_id=col_id and oset=oset;
      String exec_cmd = "python3 ../monet/python_files/q1_frac.py "+col_id+" "+oset;
      Process p = Runtime.getRuntime().exec(exec_cmd);      
      BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String output = stdInput.readLine();
      // int isthere = Integer.parseInt(output);
      int isthere = Integer.parseInt(output);

      if(isthere==-1){
         JobConf conf = new JobConf(CountAvgIL_Frac.class); 
         conf.set("col_id", args[2]);
         conf.set("oset", args[3]);
         conf.setJobName("count-average"); 
         conf.setOutputKeyClass(Text.class);
         conf.setOutputValueClass(IntWritable.class); 
         conf.setMapperClass(E_EMapper.class); 
         conf.setCombinerClass(E_EReduce.class); 
         conf.setReducerClass(E_EReduce.class); 
         conf.setInputFormat(TextInputFormat.class); 
         conf.setOutputFormat(TextOutputFormat.class); 
         
         FileInputFormat.setInputPaths(conf, new Path(args[0])); 
         FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
         
         JobClient.runJob(conf);

         // q4 - update catalog set col_id=1
         // q4_frac - update catalog set loaded=1 where col_id=col_id and oset=oset;
         String exec_cmd3 = "python3 ../monet/python_files/q4_frac.py "+col_id+" "+oset;
         p = Runtime.getRuntime().exec(exec_cmd3);


      }else{

         int oset_int = Integer.parseInt(oset);
         int lrange = (oset_int-1)*2000;
         int hrange = oset_int*2000 -1;

         // q2 - select avg(col_id) from main_table;
         // q2_frac - select avg(col_id) from main_table where ID between lrange and hrange;
         String exec_cmd1 = "python3 ../monet/python_files/q2_frac.py "+col_id+" "+lrange+" "+hrange;
         p = Runtime.getRuntime().exec(exec_cmd1);
         BufferedReader stdInput2 = new BufferedReader(new InputStreamReader(p.getInputStream()));
         String output2 = stdInput2.readLine();
         // int avg = Integer.parseInt(output2);
         
         System.out.println("average "+output2);

      }
      System.out.println("\nExecution Complete!\n");
      long etime = System.currentTimeMillis();
      float sec = (etime - stime)/1000F;
      System.out.println("\n\nQET: "+ sec);
   } 
} 