package xyz.ieden.flink.simple;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 通过批处理实现 WorkCount
 *
 * @author gavin
 * @version 1.0
 * @datetime 2020/12/8 21:42
 */
public class WorkCountBybBatch {
    public static void main(String[] args) throws Exception {
        String filePath = Thread.currentThread().getContextClassLoader().getResource("work.txt").getPath();

        // 获取运行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        // 读取文件获取数据源
        DataSource<String> dataSource = environment.readTextFile(filePath, "UTF-8");

        // 得到聚合算子
        AggregateOperator<Tuple2<String, Integer>> operator = dataSource.flatMap((String val, Collector<Tuple2<String, Integer>> collector) -> {
            if (StringUtils.isNotBlank(val)) {
                // 分割单词
                String[] strArr = val.split(" ");
                for (String str : strArr) {
                    // 聚合
                    collector.collect(new Tuple2<>(str, 1));
                }
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).groupBy(0).sum(1);

        // 遍历
        operator.print();
    }
}
