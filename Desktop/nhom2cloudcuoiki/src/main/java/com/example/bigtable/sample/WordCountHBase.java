/**
 *
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
package com.example.bigtable.sample;

import java.io.IOException;
import java.util.Collections;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountHBase {

  public static final byte[] COLUMN_FAMILY = "cf".getBytes();

  public static final byte[] COUNT_COLUMN_NAME = "count".getBytes();

  public static class TokenizerMapper extends
          Mapper<Object, Text, ImmutableBytesWritable, IntWritable> {

    // giá trị là 1 khi gán cho mỗi từ
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {
      tringTokenizer itr = new StringTokenizer(value.toString());
      ImmutableBytesWritable word = new ImmutableBytesWritable();


        word.set(Bytes.toBytes(itr.nextToken()));
      
      while (itr.hasMoreTokens()) {
        String token = itr.nextToken();
        if(token.length() >= 4) {
          word.set(Bytes.toBytes(token));
          // gán cặp key-value
          context.write(word, one);
        }
      }
    }
  }


  public static class MyTableReducer extends
          TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
    public int sum(Iterable<IntWritable> values) {
      int i = 0;
      for (IntWritable val : values) {
        i += val.get();
      }
      return i;
    }

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

      int sum = sum(values);

      
      Put put = new Put(key.get());

           put.addColumn(COLUMN_FAMILY, COUNT_COLUMN_NAME, Bytes.toBytes(sum));

      // ghi dữ liệu vào bảng
      context.write(null, put);
    }

  }

  public static void main(String[] args) throws Exception {

    nối đến bigtable instance
    Configuration conf = HBaseConfiguration.create();

    
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("\nLỗi cú pháp: wordcount-hbase <in> [<in>...] <table-name>\n");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "Đếm từ khoá");

    
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }

    TableName tableName = TableName.valueOf(otherArgs[otherArgs.length - 1]);
    try {
      CreateTable.createTable(tableName, conf,
          Collections.singletonList(Bytes.toString(COLUMN_FAMILY)));
    } catch (Exception e) {
      System.out.println("\nGặp lỗi trong quá trình tạo table trong HBase!\n");
    }

        job.setJarByClass(WordCountHBase.class);

    
    job.setMapperClass(TokenizerMapper.class);

của key: IntWritable
    job.setMapOutputValueClass(IntWritable.class);    TableMapReduceUtil.initTableReducerJob(tableName.getNameAsString(), MyTableReducer.class, job);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    System.out.println("\nKết quả đã được đưa vào table 'wordcount-result-group4' hoàn tất! Nếu bạn thấy thông báo này tức là chương trình đã hoàn thành!\n");
  }
}

