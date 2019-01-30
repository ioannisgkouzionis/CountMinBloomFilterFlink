package count_min.count_min;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;

import javax.security.auth.login.Configuration;

public class CountMinSt extends RichMapFunction<Count_Min, Tuple2<Long, Long>> {
    private transient ValueState<Count_Min> cmst;
    @Override
    public Tuple2<Long, Long> map(Count_Min count_min) throws Exception{
        Count_Min curCM = cmst.value();
        curCM.add(count_min.item, count_min.count);
        cmst.update(curCM);
        return Tuple2.of(count_min.item,count_min.count);
    }
    public void open(Configuration config) {
        ValueStateDescriptor<Count_Min> descriptor =
                new ValueStateDescriptor<>(
                        "countminstate",
                        Count_Min.class);
        cmst = getRuntimeContext().getState(descriptor);

    }
}
