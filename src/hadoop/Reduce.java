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

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

public class Reduce extends Reducer<Text, CompositeWritable, DBOutputWritable, NullWritable>
{
   protected void reduce(Text key, Iterable<CompositeWritable> values, Context context)
   {
      int intKey = Integer.parseInt(key.toString());
      LinkedList<CompositeWritable> valuesList = new LinkedList<CompositeWritable>();

      // Copy Iterable data on a LinkedList so we can work on it.
      for(CompositeWritable value : values)
      {
         valuesList.add(new CompositeWritable(value.getVarName()));
      }

      // UPDATE or INSERT the new data
      try
      {
         for (CompositeWritable element : valuesList)
         {
            context.write(new DBOutputWritable(intKey+element.getVarName()), NullWritable.get());
         }
      }
      catch(IOException e)
      {
         e.printStackTrace();
      }
      catch(InterruptedException e)
      {
         e.printStackTrace();
      }
   }
}
