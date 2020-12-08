package xyz.ieden.flink.simple;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 通过流实现 WorkCount
 *
 * @author gavin
 * @version 1.0
 * @datetime 2020/12/8 21:42
 */
public class WorkCountBybStreaming {
    public static void main(String[] args) throws Exception {
        String filePath = Thread.currentThread().getContextClassLoader().getResource("work.txt").getPath();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port", 8888);
        // 获取流执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置平行度
        environment.setParallelism(5);

        // 获取数据流源
        DataStreamSource<String> streamSource = environment.socketTextStream(host, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = streamSource.flatMap((String val, Collector<Tuple2<String, Integer>> collector) -> {
            if (StringUtils.isNotBlank(val)) {
                // 分割单词
                String[] strArr = val.split(" ");
                for (String str : strArr) {
                    // 聚合
                    collector.collect(new Tuple2<>(str, 1));
                }
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).sum(1);

        operator.print();

        // 执行环境
        environment.execute();

    }
}
