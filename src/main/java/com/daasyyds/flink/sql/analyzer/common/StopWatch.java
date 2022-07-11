package com.daasyyds.flink.sql.analyzer.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StopWatch {
    private static final Logger logger = LoggerFactory.getLogger(StopWatch.class);
    private long baseTimestamp;

    private StopWatch() {
        this.baseTimestamp = System.currentTimeMillis();
    }

    public static StopWatch start() {
        return new StopWatch();
    }

    public final long stop() {
        long cost = System.currentTimeMillis() - baseTimestamp;
        logger.debug("stop watch start at timestamp {}, cost {} ms", baseTimestamp, cost);
        return cost;
    }

}
