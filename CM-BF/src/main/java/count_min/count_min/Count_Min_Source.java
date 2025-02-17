package count_min.count_min;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;


public class Count_Min_Source  implements SourceFunction<Count_Min> {
    private final int maxDelayMsecs;
    private final int watermarkDelayMSecs;

    private final String dataFilePath;
    private final int servingSpeed;
    private transient BufferedReader reader;

    public Count_Min_Source(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
        if(maxEventDelaySecs < 0) {
            throw new IllegalArgumentException("Max event delay must be positive");
        }
        this.dataFilePath = dataFilePath;
        this.maxDelayMsecs = maxEventDelaySecs * 1000;
        this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;
        this.servingSpeed = servingSpeedFactor;
    }


    public void run(SourceContext<Count_Min> sourceContext) throws Exception {


        reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilePath), "UTF-8"));

        generateUnorderedStream(sourceContext);

        this.reader.close();
        this.reader = null;


    }

    private void generateUnorderedStream(SourceContext<Count_Min> sourceContext) throws Exception {

//        long servingStartTime = Calendar.getInstance().getTimeInMillis();
//        long dataStartTime;

//        Random rand = new Random(7452);
//        PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<Tuple2<Long, Object>>(
//                32,
//                new Comparator<Tuple2<Long, Object>>() {
//                    @Override
//                    public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
//                        return o1.f0.compareTo(o2.f0);
//                    }
//                });

        // read first ride and insert it into emit schedule
        String line;
        Count_Min ride;
        if (reader.ready() && (line = reader.readLine()) != null) {
            // read first ride
            ride = Count_Min.fromString(line);
            // extract starting timestamp
//            dataStartTime = getEventTime(ride);
            // get delayed time
//            long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);

//            emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));
            // schedule next watermark
//            long watermarkTime = dataStartTime + watermarkDelayMSecs;
//            Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
//            emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));

        } else {
            return;
        }

        // peek at next ride
        if (reader.ready() && (line = reader.readLine()) != null) {
            ride = Count_Min.fromString(line);
        }

        // read rides one-by-one and emit a random ride from the buffer each time
//        while (emitSchedule.size() > 0 || reader.ready()) {
        while (reader.ready()) {

            // insert all events into schedule that might be emitted next
//            long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
//            long rideEventTime = ride != null ? getEventTime(ride) : -1;
//            while(
//                    ride != null && ( // while there is a ride AND
//                            emitSchedule.isEmpty() || // and no ride in schedule OR
//                                    rideEventTime < curNextDelayedEventTime + maxDelayMsecs) // not enough rides in schedule
//            )
//            {
//                // insert event into emit schedule
//                long delayedEventTime = rideEventTime + getNormalDelayMsecs(rand);
//                emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));

                // read next ride
                if (reader.ready() && (line = reader.readLine()) != null) {
                    ride = Count_Min.fromString(line);
//                    rideEventTime = getEventTime(ride);
                }
                else {
                    ride = null;
//                    rideEventTime = -1;
                }
            }

            // emit schedule is updated, emit next element in schedule
//            Tuple2<Long, Object> head = emitSchedule.poll();
//            long delayedEventTime = head.f0;
//
//            long now = Calendar.getInstance().getTimeInMillis();
//            long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
//            long waitTime = servingTime - now;

//            Thread.sleep( (waitTime > 0) ? waitTime : 0);
//
//            if(head.f1 instanceof Count_Min) {
//                Count_Min emitRide = (Count_Min) head.f1;
//                // emit ride
//                sourceContext.collectWithTimestamp(emitRide, getEventTime(emitRide));
//            }
//            else if(head.f1 instanceof Watermark) {
//                Watermark emitWatermark = (Watermark)head.f1;
//                // emit watermark
//                sourceContext.emitWatermark(emitWatermark);
//                // schedule next watermark
//                long watermarkTime = delayedEventTime + watermarkDelayMSecs;
//                Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
//                emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
//            }

    }

    public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    public long getEventTime(Count_Min ride) {
        return ride.getEventTime();
    }

    public long getNormalDelayMsecs(Random rand) {
        long delay = -1;
        long x = maxDelayMsecs / 2;
        while(delay < 0 || delay > maxDelayMsecs) {
            delay = (long)(rand.nextGaussian() * x) + x;
        }
        return delay;
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }

        } catch(IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
        }
    }


}
