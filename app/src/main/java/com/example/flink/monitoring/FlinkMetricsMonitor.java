package com.example.flink.monitoring;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codahale.metrics.SlidingWindowReservoir;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Comprehensive monitoring and metrics for Flink advanced applications
 * 
 * Provides:
 * - Performance monitoring
 * - Business metrics
 * - Error tracking
 * - Latency measurements
 * - Throughput monitoring
 */
public class FlinkMetricsMonitor {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkMetricsMonitor.class);
    
    /**
     * Monitored Map Function with comprehensive metrics
     */
    public static class MonitoredMapFunction<T, R> extends RichMapFunction<T, R> {
        
        private transient Counter recordsProcessed;
        private transient Counter errorCount;
        private transient Meter throughputMeter;
        private transient Histogram processingLatency;
        private transient Gauge<Long> lastProcessedTime;
        private transient AtomicLong lastProcessedTimestamp;
        
        private final String operatorName;
        private final MapFunction<T, R> actualFunction;
        
        public MonitoredMapFunction(String operatorName, MapFunction<T, R> actualFunction) {
            this.operatorName = operatorName;
            this.actualFunction = actualFunction;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            MetricGroup metricGroup = getRuntimeContext()
                .getMetricGroup()
                .addGroup("flink_advanced")
                .addGroup("operators")
                .addGroup(operatorName);
            
            // Initialize metrics
            recordsProcessed = metricGroup.counter("records_processed");
            errorCount = metricGroup.counter("errors");
            
            throughputMeter = metricGroup.meter("throughput", 
                new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
            
            processingLatency = metricGroup.histogram("processing_latency_ms",
                new DropwizardHistogramWrapper(
                    new com.codahale.metrics.Histogram(new SlidingWindowReservoir(1000))));
            
            lastProcessedTimestamp = new AtomicLong(System.currentTimeMillis());
            lastProcessedTime = metricGroup.gauge("last_processed_time", 
                () -> lastProcessedTimestamp.get());
            
            LOG.info("Initialized metrics for operator: {}", operatorName);
        }
        
        @Override
        public R map(T input) throws Exception {
            long startTime = System.currentTimeMillis();
            
            try {
                // Execute actual function
                R result = actualFunction.map(input);
                
                // Update success metrics
                recordsProcessed.inc();
                throughputMeter.markEvent();
                lastProcessedTimestamp.set(System.currentTimeMillis());
                
                long processingTime = System.currentTimeMillis() - startTime;
                processingLatency.update(processingTime);
                
                return result;
                
            } catch (Exception e) {
                // Update error metrics
                errorCount.inc();
                LOG.error("Error processing record in operator {}: {}", operatorName, e.getMessage(), e);
                throw e;
            }
        }
    }
    
    /**
     * Monitored FlatMap Function for more complex processing
     */
    public static class MonitoredFlatMapFunction<T, R> extends RichFlatMapFunction<T, R> {
        
        private transient Counter recordsProcessed;
        private transient Counter recordsEmitted;
        private transient Counter errorCount;
        private transient Meter inputThroughputMeter;
        private transient Meter outputThroughputMeter;
        private transient Histogram processingLatency;
        private transient Gauge<Double> fanOutRatio;
        private transient AtomicLong totalInput;
        private transient AtomicLong totalOutput;
        
        private final String operatorName;
        private final FlatMapFunction<T, R> actualFunction;
        
        public MonitoredFlatMapFunction(String operatorName, FlatMapFunction<T, R> actualFunction) {
            this.operatorName = operatorName;
            this.actualFunction = actualFunction;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            MetricGroup metricGroup = getRuntimeContext()
                .getMetricGroup()
                .addGroup("flink_advanced")
                .addGroup("operators")
                .addGroup(operatorName);
            
            // Initialize metrics
            recordsProcessed = metricGroup.counter("records_processed");
            recordsEmitted = metricGroup.counter("records_emitted");
            errorCount = metricGroup.counter("errors");
            
            inputThroughputMeter = metricGroup.meter("input_throughput",
                new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
            
            outputThroughputMeter = metricGroup.meter("output_throughput",
                new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
            
            processingLatency = metricGroup.histogram("processing_latency_ms",
                new DropwizardHistogramWrapper(
                    new com.codahale.metrics.Histogram(new SlidingWindowReservoir(1000))));
            
            totalInput = new AtomicLong(0);
            totalOutput = new AtomicLong(0);
            
            fanOutRatio = metricGroup.gauge("fan_out_ratio", () -> {
                long input = totalInput.get();
                long output = totalOutput.get();
                return input > 0 ? (double) output / input : 0.0;
            });
            
            LOG.info("Initialized metrics for flatmap operator: {}", operatorName);
        }
        
        @Override
        public void flatMap(T input, Collector<R> collector) throws Exception {
            long startTime = System.currentTimeMillis();
            long outputCountBefore = totalOutput.get();
            
            try {
                // Wrap collector to count outputs
                MonitoringCollector<R> monitoringCollector = new MonitoringCollector<>(collector, totalOutput);
                
                // Execute actual function
                actualFunction.flatMap(input, monitoringCollector);
                
                // Update metrics
                recordsProcessed.inc();
                totalInput.incrementAndGet();
                inputThroughputMeter.markEvent();
                
                long outputCountAfter = totalOutput.get();
                long emittedInThisCall = outputCountAfter - outputCountBefore;
                recordsEmitted.inc(emittedInThisCall);
                
                for (int i = 0; i < emittedInThisCall; i++) {
                    outputThroughputMeter.markEvent();
                }
                
                long processingTime = System.currentTimeMillis() - startTime;
                processingLatency.update(processingTime);
                
            } catch (Exception e) {
                errorCount.inc();
                LOG.error("Error processing record in flatmap operator {}: {}", operatorName, e.getMessage(), e);
                throw e;
            }
        }
        
        @FunctionalInterface
        public interface FlatMapFunction<T, R> {
            void flatMap(T input, Collector<R> collector) throws Exception;
        }
    }
    
    /**
     * Collector wrapper that counts emitted records
     */
    private static class MonitoringCollector<T> implements Collector<T> {
        private final Collector<T> delegate;
        private final AtomicLong counter;
        
        public MonitoringCollector(Collector<T> delegate, AtomicLong counter) {
            this.delegate = delegate;
            this.counter = counter;
        }
        
        @Override
        public void collect(T record) {
            delegate.collect(record);
            counter.incrementAndGet();
        }
        
        @Override
        public void close() {
            delegate.close();
        }
    }
    
    /**
     * Business metrics tracker for domain-specific monitoring
     */
    public static class BusinessMetricsTracker extends RichMapFunction<Object, Object> {
        
        private transient Counter totalRevenue;
        private transient Counter totalOrders;
        private transient Counter totalUsers;
        private transient Histogram orderValue;
        private transient Meter revenueRate;
        private transient Gauge<Double> averageOrderValue;
        private transient AtomicLong totalRevenueAmount;
        private transient AtomicLong totalOrderCount;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            MetricGroup businessMetrics = getRuntimeContext()
                .getMetricGroup()
                .addGroup("business")
                .addGroup("ecommerce");
            
            totalRevenue = businessMetrics.counter("total_revenue_cents");
            totalOrders = businessMetrics.counter("total_orders");
            totalUsers = businessMetrics.counter("total_users");
            
            orderValue = businessMetrics.histogram("order_value_cents",
                new DropwizardHistogramWrapper(
                    new com.codahale.metrics.Histogram(new SlidingWindowReservoir(10000))));
            
            revenueRate = businessMetrics.meter("revenue_rate",
                new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
            
            totalRevenueAmount = new AtomicLong(0);
            totalOrderCount = new AtomicLong(0);
            
            averageOrderValue = businessMetrics.gauge("average_order_value_cents", () -> {
                long orders = totalOrderCount.get();
                long revenue = totalRevenueAmount.get();
                return orders > 0 ? (double) revenue / orders : 0.0;
            });
            
            LOG.info("Initialized business metrics tracker");
        }
        
        @Override
        public Object map(Object input) throws Exception {
            // This would be customized based on the actual event types
            // For now, just pass through
            return input;
        }
        
        /**
         * Track a purchase event
         */
        public void trackPurchase(String userId, double amount) {
            totalOrders.inc();
            long amountCents = (long) (amount * 100);
            totalRevenue.inc(amountCents);
            totalRevenueAmount.addAndGet(amountCents);
            totalOrderCount.incrementAndGet();
            orderValue.update(amountCents);
            revenueRate.markEvent();
        }
        
        /**
         * Track a new user
         */
        public void trackNewUser() {
            totalUsers.inc();
        }
    }
    
    /**
     * Performance monitoring utilities
     */
    public static class PerformanceMonitor {
        
        /**
         * Monitor checkpoint duration
         */
        public static void monitorCheckpointDuration(long durationMs, MetricGroup metricGroup) {
            Histogram checkpointDuration = metricGroup.histogram("checkpoint_duration_ms",
                new DropwizardHistogramWrapper(
                    new com.codahale.metrics.Histogram(new SlidingWindowReservoir(100))));
            
            checkpointDuration.update(durationMs);
            
            if (durationMs > 30000) { // More than 30 seconds
                LOG.warn("Long checkpoint detected: {}ms", durationMs);
            }
        }
        
        /**
         * Monitor processing lag
         */
        public static void monitorProcessingLag(long eventTime, long processingTime, MetricGroup metricGroup) {
            long lag = processingTime - eventTime;
            
            Histogram processingLag = metricGroup.histogram("processing_lag_ms",
                new DropwizardHistogramWrapper(
                    new com.codahale.metrics.Histogram(new SlidingWindowReservoir(1000))));
            
            processingLag.update(lag);
            
            if (lag > 60000) { // More than 1 minute lag
                LOG.warn("High processing lag detected: {}ms", lag);
            }
        }
        
        /**
         * Monitor memory usage
         */
        public static void monitorMemoryUsage(MetricGroup metricGroup) {
            Gauge<Long> heapUsed = metricGroup.gauge("heap_memory_used", () -> {
                Runtime runtime = Runtime.getRuntime();
                return runtime.totalMemory() - runtime.freeMemory();
            });
            
            Gauge<Long> heapMax = metricGroup.gauge("heap_memory_max", () -> {
                return Runtime.getRuntime().maxMemory();
            });
            
            Gauge<Double> heapUtilization = metricGroup.gauge("heap_utilization_percent", () -> {
                Runtime runtime = Runtime.getRuntime();
                long used = runtime.totalMemory() - runtime.freeMemory();
                long max = runtime.maxMemory();
                return max > 0 ? (double) used / max * 100 : 0.0;
            });
        }
    }
    
    /**
     * Alert manager for threshold-based monitoring
     */
    public static class AlertManager {
        
        private static final Logger ALERT_LOG = LoggerFactory.getLogger("ALERTS");
        
        /**
         * Check error rate threshold
         */
        public static void checkErrorRate(double errorRate, double threshold) {
            if (errorRate > threshold) {
                ALERT_LOG.error("HIGH ERROR RATE ALERT: Current rate {}% exceeds threshold {}%", 
                               errorRate * 100, threshold * 100);
            }
        }
        
        /**
         * Check processing lag threshold
         */
        public static void checkProcessingLag(long lagMs, long thresholdMs) {
            if (lagMs > thresholdMs) {
                ALERT_LOG.warn("HIGH PROCESSING LAG ALERT: Current lag {}ms exceeds threshold {}ms", 
                              lagMs, thresholdMs);
            }
        }
        
        /**
         * Check throughput threshold
         */
        public static void checkThroughput(double currentThroughput, double minimumThroughput) {
            if (currentThroughput < minimumThroughput) {
                ALERT_LOG.warn("LOW THROUGHPUT ALERT: Current throughput {} is below minimum {}", 
                              currentThroughput, minimumThroughput);
            }
        }
    }
}
