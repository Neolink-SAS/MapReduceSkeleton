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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CompositeWritable implements Writable
{
   private int varName = 0;

   @SuppressWarnings("unused")
   public CompositeWritable() {}

   public CompositeWritable(int varName)
   {
      this.varName = varName;
   }

   @Override
   public void readFields(DataInput in) throws IOException
   {
      varName = in.readInt();
   }

   @Override
   public void write(DataOutput out) throws IOException
   {
      out.writeInt(varName);
   }

   public int getVarName()
   {
      return this.varName;
   }
}
