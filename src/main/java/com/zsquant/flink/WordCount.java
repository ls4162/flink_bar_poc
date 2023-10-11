package com.zsquant.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 输入数据
        DataSet<String> text = env.fromElements(
                "Hello Flink",
                "Hello World",
                "Hello Flink",
                "Hello Java"
        );

        DataSet<Tuple2<String, Integer>> counts =
                // 将输入字符串拆分为单词
                text.flatMap(new Tokenizer())
                        // 按单词分组
                        .groupBy(0)
                        // 对每个组的单词计数
                        .sum(1);

        // 打印结果
        counts.print();
    }

    // 用户自定义函数
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 使用空格分割字符串
            String[] words = value.split("\\s");
            // 为每个单词返回一个(word,1)的二元组
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}

