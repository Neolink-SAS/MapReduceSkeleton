/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class Main extends Configured implements Tool
{
   private enum Type {UPDATE, INSERT, DELINSERT};

   public static void main(String[] args) throws Exception
   {
      int res = ToolRunner.run(new Configuration(), new Main(), args);
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception
   {
      // Configuration generale
      Configuration conf = this.getConf();
      String jobName = "JobName";

      // SQL credentials
      conf.set("dbServer", args[0]);
      conf.set("dbLogin", args[1]);
      conf.set("dbPassword", args[2]);

      // SQL query of the input dataset
      String sqlQuery = "sqlQuery";

      // SQL out parameters
      Type outType = Type.INSERT;
      String outTable = "OutTable";
      String[] outFields = {"ColOne", "colTwo", "Etc..."};

      /***************************************
       * No need to edit below!               *
       ***************************************/

      // Armageddon on the out table if outType = DELINSERT
      if (outType == Type.DELINSERT)
      {
         Connection connection = DriverManager.getConnection(conf.get("dbServer"), conf.get("dbLogin"), conf.get("dbPassword"));
         Statement requeteSuppression = connection.createStatement();
         requeteSuppression.executeUpdate("DELETE FROM " + outTable);
      }

      // Configuration de la DB
      DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", conf.get("dbServer"), conf.get("dbLogin"), conf.get("dbPassword"));

      // Configuration de base du job
      Job job = Job.getInstance(conf, jobName);
      job.setJarByClass(Main.class);
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(CompositeWritable.class);
      job.setOutputKeyClass(DBOutputWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setInputFormatClass(DBInputFormat.class);

      // OutputFormatClass configuration depending of the query type
      if (outType == Type.INSERT || outType == Type.DELINSERT)
      {
         job.setOutputFormatClass(DBOutputFormat.class);
      }
      else if (outType == Type.UPDATE)
      {
         job.setOutputFormatClass(MysqlDBOutputFormat.class);
      }

      // (Mysql)DBoutputFormat configuration depending of the query type
      DBInputFormat.setInput(job, DBInputWritable.class, sqlQuery, "SELECT COUNT(*) FROM ("+sqlQuery+") AS nbcols");
      if (outType == Type.INSERT || outType == Type.DELINSERT)
      {
         DBOutputFormat.setOutput(job, outTable, outFields);
      }
      else if (outType == Type.UPDATE)
      {
         MysqlDBOutputFormat.setOutput(job, outTable, outFields);
      }

      // Run the job
      return job.waitForCompletion(true)? 0:1;
   }
}
