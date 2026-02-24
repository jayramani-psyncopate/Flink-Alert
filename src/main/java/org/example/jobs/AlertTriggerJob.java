package org.example.jobs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Flink job that triggers various alert scenarios based on command-line arguments.
 * 
 * Usage: Pass alert type as first argument, e.g., "normal", "job-failing", "checkpoint-failure", etc.
 */
public class AlertTriggerJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(AlertTriggerJob.class);
    private static final long SAFETY_TIMEOUT_MS = 3 * 60 * 1000L; // 3 minutes
    private static final int CPU_BUSY_MS = 600;
    private static final int CPU_IDLE_MS = 400;
    private static final Object MEMORY_PRESSURE_LOCK = new Object();
    private static final List<byte[]> MEMORY_PRESSURE_BUFFERS = new ArrayList<>();
    private static long retainedMemoryBytes = 0L;
    private static final Object JM_MEMORY_PRESSURE_LOCK = new Object();
    private static final List<byte[]> JM_MEMORY_PRESSURE_BUFFERS = new ArrayList<>();
    private static volatile boolean jmCpuPressureStarted = false;
    private static volatile boolean jmMemoryPressureStarted = false;
    private static volatile long stressEndAtMs = Long.MAX_VALUE;
    
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: AlertTriggerJob <alert-type> [additional-params...]");
            System.err.println("Available alert types:");
            System.err.println("  - normal: Runs in normal mode without intentionally triggering alerts");
            System.err.println("  - job-failing: Simulates job failures");
            System.err.println("  - job-restarting: Causes repeated restarts");
            System.err.println("  - checkpoint-failure: Causes checkpoint failures");
            System.err.println("  - checkpoint-stuck: Simulates stuck checkpoints");
            System.err.println("  - checkpoint-duration-high: Increases checkpoint duration");
            System.err.println("  - cpu-high: Simulates high CPU usage");
            System.err.println("  - memory-high: Simulates high memory usage");
            System.err.println("  - jobmanager-cpu-high: Simulates high JobManager CPU usage");
            System.err.println("  - jobmanager-memory-high: Simulates high JobManager heap usage");
            System.err.println("  - restart-rate-high: Causes high restart rate");
            System.exit(1);
        }
        
        String alertType = args[0];
        LOG.info("Starting AlertTriggerJob with alert type: {}", alertType);
        configureSafetyTimeout(alertType);
        startJobManagerPressureIfNeeded(alertType);
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        if (isCheckpointScenario(alertType)) {
            // Enable checkpoints only for checkpoint-related scenarios to reduce noise elsewhere.
            env.enableCheckpointing(10000); // 10 second interval
            if ("checkpoint-stuck".equals(alertType)) {
                // Allow overlapping checkpoints so the "in-progress checkpoints" metric can reach 2.
                env.enableCheckpointing(2000); // trigger more frequently for this scenario
                env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
                LOG.info("Configured checkpoint-stuck scenario with checkpoint interval=2s and maxConcurrentCheckpoints=2");
            }
        }
        
        DataStream<String> source = env.addSource(new AlertSourceFunction(alertType))
                .name("alert-source");
        
        // Route to appropriate alert handler based on type
        DataStream<String> processed = routeToAlertHandler(source, alertType, args);
        
        // Sink to print (or you can configure Kafka sink)
        processed.print().name("alert-output");
        
        env.execute("AlertTriggerJob");
    }
    
    private static DataStream<String> routeToAlertHandler(
            DataStream<String> source, String alertType, String[] args) {
        
        switch (alertType) {
            case "normal":
                return handleNormal(source);
            case "job-failing":
                return handleJobFailing(source);
            case "job-restarting":
                return handleJobRestarting(source);
            case "checkpoint-failure":
                return handleCheckpointFailure(source);
            case "checkpoint-stuck":
                return handleCheckpointStuck(source);
            case "checkpoint-duration-high":
                return handleCheckpointDurationHigh(source);
            case "cpu-high":
                return handleCpuHigh(source);
            case "memory-high":
                return handleMemoryHigh(source);
            case "jobmanager-cpu-high":
                return handleJobManagerCpuHigh(source);
            case "jobmanager-memory-high":
                return handleJobManagerMemoryHigh(source);
            case "restart-rate-high":
                return handleRestartRateHigh(source);
            default:
                LOG.warn("Unknown alert type: {}, running in normal mode", alertType);
                return handleNormal(source);
        }
    }
    
    // Alert Handler Implementations

    private static DataStream<String> handleNormal(DataStream<String> source) {
        LOG.info("Running in normal mode (no intentional alert triggers)");
        return source.map(value -> "Normal mode: " + value).name("normal-map");
    }
    
    private static DataStream<String> handleJobFailing(DataStream<String> source) {
        LOG.info("Triggering job-failing alert scenario");
        return source.map(new MapFunction<String, String>() {
            private int count = 0;
            @Override
            public String map(String value) throws Exception {
                count++;
                if (isStressActive() && count > 100) {
                    LOG.error("Simulating job failure - throwing exception");
                    throw new RuntimeException("Simulated job failure for alert testing");
                }
                return "Processing: " + value;
            }
        }).name("job-failing-map");
    }
    
    private static DataStream<String> handleJobRestarting(DataStream<String> source) {
        LOG.info("Triggering job-restarting alert scenario");
        return source.map(new MapFunction<String, String>() {
            private int count = 0;
            @Override
            public String map(String value) throws Exception {
                count++;
                // Fail every 50 records to cause restarts
                if (isStressActive() && count % 50 == 0) {
                    LOG.error("Simulating restart scenario - throwing exception");
                    throw new RuntimeException("Simulated restart failure");
                }
                return "Processing: " + value;
            }
        }).name("job-restarting-map");
    }
    
    private static DataStream<String> handleCheckpointFailure(DataStream<String> source) {
        LOG.info("Triggering checkpoint-failure alert scenario");
        return source.keyBy(s -> s.hashCode() % 10)
                .map(new RichMapFunction<String, String>() {
                    private transient ValueState<Integer> state;
                    private int count = 0;
                    
                    @Override
                    public void open(Configuration parameters) {
                        state = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("failing-state", Integer.class));
                    }
                    
                    @Override
                    public String map(String value) throws Exception {
                        count++;
                        // Periodically cause state issues that lead to checkpoint failures
                        if (count % 20 == 0) {
                            // Access state in a way that might cause serialization issues
                            Integer current = state.value();
                            state.update(current == null ? 1 : current + 1);
                        }
                        // Simulate checkpoint failure by throwing during state access
                        if (isStressActive() && count % 100 == 0) {
                            throw new RuntimeException("Simulated checkpoint failure");
                        }
                        return "Checkpoint test: " + value;
                    }
                }).name("checkpoint-failure-map");
    }
    
    private static DataStream<String> handleCheckpointStuck(DataStream<String> source) {
        LOG.info("Triggering checkpoint-stuck alert scenario");
        return source.keyBy(s -> s.hashCode() % 10)
                .map(new RichMapFunction<String, String>() {
                    private transient ValueState<String> state;
                    
                    @Override
                    public void open(Configuration parameters) {
                        state = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("stuck-state", String.class));
                    }
                    
                    @Override
                    public String map(String value) throws Exception {
                        // Hold state for long time to simulate stuck checkpoint
                        state.update(value);
                        if (isStressActive()) {
                            Thread.sleep(5000); // Slow processing to cause checkpoint delays
                        }
                        return "Stuck checkpoint test: " + value;
                    }
                }).name("checkpoint-stuck-map");
    }
    
    private static DataStream<String> handleCheckpointDurationHigh(DataStream<String> source) {
        LOG.info("Triggering checkpoint-duration-high alert scenario");
        return source.keyBy(s -> s.hashCode() % 10)
                .map(new RichMapFunction<String, String>() {
                    private transient ValueState<String> state;
                    
                    @Override
                    public void open(Configuration parameters) {
                        state = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("duration-state", String.class));
                    }
                    
                    @Override
                    public String map(String value) throws Exception {
                        // Large state updates to increase checkpoint duration during the active window.
                        StringBuilder largeValue = new StringBuilder(value);
                        int iterations = isStressActive() ? 1000 : 50;
                        for (int i = 0; i < iterations; i++) {
                            largeValue.append("data").append(i);
                        }
                        state.update(largeValue.toString());
                        return "High duration test: " + value;
                    }
                }).name("checkpoint-duration-map");
    }
    
    private static DataStream<String> handleCpuHigh(DataStream<String> source) {
        LOG.info("Triggering cpu-high alert scenario");
        return source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (!isStressActive()) {
                    return "CPU normal: " + value;
                }

                // Approximate 60% load using duty-cycled busy work.
                long sum = 0;
                long endNanos = System.nanoTime() + (CPU_BUSY_MS * 1_000_000L);
                long i = 1;
                while (System.nanoTime() < endNanos) {
                    sum += (i * i) % 97;
                    i++;
                }
                Thread.sleep(CPU_IDLE_MS);
                return "CPU intensive: " + value + " sum=" + sum;
            }
        }).name("cpu-high-map");
    }
    
    private static DataStream<String> handleMemoryHigh(DataStream<String> source) {
        LOG.info("Triggering memory-high alert scenario");
        return source.map(new MapFunction<String, String>() {
            private boolean initialized = false;
            private long lowWatermarkBytes;
            private long highWatermarkBytes;
            private long hardLimitBytes;

            @Override
            public String map(String value) throws Exception {
                if (!initialized) {
                    long maxHeap = Runtime.getRuntime().maxMemory();
                    lowWatermarkBytes = (long) (maxHeap * 0.55);
                    highWatermarkBytes = (long) (maxHeap * 0.59);
                    hardLimitBytes = (long) (maxHeap * 0.60);
                    initialized = true;
                    LOG.info(
                            "memory-high target heap band low={} high={} hardLimit={} (maxHeap={})",
                            lowWatermarkBytes, highWatermarkBytes, hardLimitBytes, maxHeap);
                }

                if (!isStressActive()) {
                    synchronized (MEMORY_PRESSURE_LOCK) {
                        MEMORY_PRESSURE_BUFFERS.clear();
                        retainedMemoryBytes = 0L;
                    }
                    return "Memory normal: " + value;
                }

                synchronized (MEMORY_PRESSURE_LOCK) {
                    long usedHeap = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

                    while (usedHeap > highWatermarkBytes && !MEMORY_PRESSURE_BUFFERS.isEmpty()) {
                        byte[] removed = MEMORY_PRESSURE_BUFFERS.remove(MEMORY_PRESSURE_BUFFERS.size() - 1);
                        retainedMemoryBytes -= removed.length;
                        usedHeap -= removed.length;
                    }

                    while (usedHeap < lowWatermarkBytes && usedHeap < hardLimitBytes) {
                        byte[] chunk = new byte[1 * 1024 * 1024];
                        for (int i = 0; i < chunk.length; i += 4096) {
                            chunk[i] = (byte) (i % 256);
                        }
                        MEMORY_PRESSURE_BUFFERS.add(chunk);
                        retainedMemoryBytes += chunk.length;
                        usedHeap += chunk.length;
                    }
                }

                Thread.sleep(500);
                return "Memory intensive: " + value + " retainedBytes=" + retainedMemoryBytes;
            }
        }).name("memory-high-map");
    }

    private static DataStream<String> handleJobManagerCpuHigh(DataStream<String> source) {
        LOG.info("Triggering jobmanager-cpu-high alert scenario (JobManager pressure thread + normal stream processing)");
        return source.map(value -> "JobManager CPU high mode: " + value).name("jobmanager-cpu-high-map");
    }

    private static DataStream<String> handleJobManagerMemoryHigh(DataStream<String> source) {
        LOG.info("Triggering jobmanager-memory-high alert scenario (JobManager pressure thread + normal stream processing)");
        return source.map(value -> "JobManager memory high mode: " + value).name("jobmanager-memory-high-map");
    }
    
    private static DataStream<String> handleRestartRateHigh(DataStream<String> source) {
        LOG.info("Triggering restart-rate-high alert scenario");
        return source.map(new MapFunction<String, String>() {
            private int count = 0;
            @Override
            public String map(String value) throws Exception {
                count++;
                // Fail frequently to cause high restart rate
                if (isStressActive() && count % 30 == 0) {
                    LOG.error("Simulating high restart rate - throwing exception");
                    throw new RuntimeException("Simulated restart");
                }
                return "Restart rate test: " + value;
            }
        }).name("restart-rate-high-map");
    }
    
    /**
     * Source function that generates test data for alert scenarios
     */
    private static class AlertSourceFunction implements SourceFunction<String> {
        private volatile boolean isRunning = true;
        private final String alertType;
        private final AtomicLong counter = new AtomicLong(0);
        
        public AlertSourceFunction(String alertType) {
            this.alertType = alertType;
        }
        
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                long count = counter.incrementAndGet();
                String record = String.format("alert-test-%s-record-%d", alertType, count);
                ctx.collect(record);
                
                // Adjust emission rate based on alert type
                int delay = getDelayForAlertType(alertType);
                Thread.sleep(delay);
            }
        }
        
        private int getDelayForAlertType(String type) {
            return 100; // Default rate
        }
        
        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static void startJobManagerPressureIfNeeded(String alertType) {
        if ("jobmanager-cpu-high".equals(alertType)) {
            startJobManagerCpuPressureThread();
        } else if ("jobmanager-memory-high".equals(alertType)) {
            startJobManagerMemoryPressureThread();
        }
    }

    private static void startJobManagerCpuPressureThread() {
        if (jmCpuPressureStarted) {
            return;
        }
        synchronized (AlertTriggerJob.class) {
            if (jmCpuPressureStarted) {
                return;
            }
            jmCpuPressureStarted = true;
            int cpuThreads = Math.max(1, (Runtime.getRuntime().availableProcessors() / 2) + 1);
            LOG.info("Starting {} JobManager CPU pressure thread(s)", cpuThreads);
            for (int t = 0; t < cpuThreads; t++) {
                Thread worker = new Thread(() -> {
                    long sink = 0L;
                    while (true) {
                        if (isStressActive()) {
                            long endNanos = System.nanoTime() + (CPU_BUSY_MS * 1_000_000L);
                            while (System.nanoTime() < endNanos) {
                                sink += (System.nanoTime() % 997);
                            }
                            try {
                                Thread.sleep(CPU_IDLE_MS);
                            } catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        } else {
                            try {
                                Thread.sleep(250);
                            } catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                }, "jm-cpu-pressure-" + t);
                worker.setDaemon(true);
                worker.start();
            }
        }
    }

    private static void startJobManagerMemoryPressureThread() {
        if (jmMemoryPressureStarted) {
            return;
        }
        synchronized (AlertTriggerJob.class) {
            if (jmMemoryPressureStarted) {
                return;
            }
            jmMemoryPressureStarted = true;
            Thread worker = new Thread(() -> {
                long maxHeap = Runtime.getRuntime().maxMemory();
                long lowWatermarkBytes = (long) (maxHeap * 0.55);
                long highWatermarkBytes = (long) (maxHeap * 0.59);
                long hardLimitBytes = (long) (maxHeap * 0.60);
                long retained = 0L;
                LOG.info(
                        "Starting JobManager memory pressure thread with low={} high={} hardLimit={} (maxHeap={})",
                        lowWatermarkBytes, highWatermarkBytes, hardLimitBytes, maxHeap);
                while (true) {
                    try {
                        synchronized (JM_MEMORY_PRESSURE_LOCK) {
                            if (!isStressActive()) {
                                JM_MEMORY_PRESSURE_BUFFERS.clear();
                                retained = 0L;
                                // Let JVM recover after timeout so alert can resolve.
                                System.gc();
                            }

                            long usedHeap = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

                            // Trim retained buffers if heap usage drifts too high.
                            while (usedHeap > highWatermarkBytes && !JM_MEMORY_PRESSURE_BUFFERS.isEmpty()) {
                                byte[] removed = JM_MEMORY_PRESSURE_BUFFERS.remove(JM_MEMORY_PRESSURE_BUFFERS.size() - 1);
                                retained -= removed.length;
                                usedHeap -= removed.length;
                            }

                            // Grow gradually in small chunks so we can stop near the limit without overshooting.
                            while (usedHeap < lowWatermarkBytes && usedHeap < hardLimitBytes) {
                                byte[] chunk = new byte[1 * 1024 * 1024];
                                for (int i = 0; i < chunk.length; i += 4096) {
                                    chunk[i] = (byte) (i % 256);
                                }
                                JM_MEMORY_PRESSURE_BUFFERS.add(chunk);
                                retained += chunk.length;
                                usedHeap += chunk.length;
                            }
                        }
                    } catch (OutOfMemoryError oom) {
                        LOG.warn("JobManager memory pressure hit OOM; releasing retained buffers and backing off");
                        synchronized (JM_MEMORY_PRESSURE_LOCK) {
                            int toRemove = JM_MEMORY_PRESSURE_BUFFERS.size() / 2;
                            for (int i = 0; i < toRemove; i++) {
                                byte[] removed = JM_MEMORY_PRESSURE_BUFFERS.remove(JM_MEMORY_PRESSURE_BUFFERS.size() - 1);
                                retained -= removed.length;
                            }
                        }
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "jm-memory-pressure");
            worker.setDaemon(true);
            worker.start();
        }
    }

    private static void configureSafetyTimeout(String alertType) {
        if (isDestructiveMode(alertType)) {
            stressEndAtMs = System.currentTimeMillis() + SAFETY_TIMEOUT_MS;
            LOG.info("Safety timeout enabled for alert type {} (stress active for {} ms)", alertType, SAFETY_TIMEOUT_MS);
        } else {
            stressEndAtMs = Long.MAX_VALUE;
        }
    }

    private static boolean isStressActive() {
        return System.currentTimeMillis() < stressEndAtMs;
    }

    private static boolean isCheckpointScenario(String alertType) {
        switch (alertType) {
            case "checkpoint-failure":
            case "checkpoint-stuck":
            case "checkpoint-duration-high":
                return true;
            default:
                return false;
        }
    }

    private static boolean isDestructiveMode(String alertType) {
        switch (alertType) {
            case "job-failing":
            case "job-restarting":
            case "checkpoint-failure":
            case "checkpoint-stuck":
            case "checkpoint-duration-high":
            case "cpu-high":
            case "memory-high":
            case "jobmanager-cpu-high":
            case "jobmanager-memory-high":
            case "restart-rate-high":
                return true;
            default:
                return false;
        }
    }
}
