package com.example.flink.timeseries;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Advanced Time Series Analytics with Apache Flink
 * 
 * Demonstrates:
 * - Real-time anomaly detection in time series
 * - Trend analysis and forecasting
 * - Seasonal decomposition
 * - Change point detection
 * - Statistical process control
 * - Time series clustering and classification
 * - Multi-variate time series analysis
 */
public class TimeSeriesAnalyticsJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesAnalyticsJob.class);
    
    public static void runDemo(StreamExecutionEnvironment env) throws Exception {
        LOG.info("📈 Starting Advanced Time Series Analytics Demo");
        
        // Configure watermark strategy for time series data
        WatermarkStrategy<TimeSeriesPoint> watermarkStrategy = WatermarkStrategy
            .<TimeSeriesPoint>forBoundedOutOfOrderness(Duration.ofSeconds(10))
            .withTimestampAssigner((point, timestamp) -> point.timestamp);
        
        // Generate synthetic time series data
        DataStream<TimeSeriesPoint> timeSeries = generateTimeSeriesStream(env)
            .assignTimestampsAndWatermarks(watermarkStrategy);
        
        // Demo 1: Real-time Anomaly Detection
        runAnomalyDetection(timeSeries);
        
        // Demo 2: Trend Analysis and Forecasting
        runTrendAnalysis(timeSeries);
        
        // Demo 3: Seasonal Decomposition
        runSeasonalDecomposition(timeSeries);
        
        // Demo 4: Change Point Detection
        runChangePointDetection(timeSeries);
        
        // Demo 5: Statistical Process Control
        runStatisticalProcessControl(timeSeries);
        
        // Demo 6: Time Series Clustering
        runTimeSeriesClustering(timeSeries);
        
        LOG.info("✅ Time Series Analytics demo configured - executing...");
        env.execute("Advanced Time Series Analytics Job");
    }
    
    /**
     * Generate synthetic multi-variate time series data
     */
    private static DataStream<TimeSeriesPoint> generateTimeSeriesStream(StreamExecutionEnvironment env) {
        return env.addSource(new TimeSeriesDataGenerator())
                  .name("Time Series Data Generator");
    }
    
    /**
     * Real-time anomaly detection using statistical methods
     */
    private static void runAnomalyDetection(DataStream<TimeSeriesPoint> timeSeries) {
        LOG.info("🚨 Running Real-time Anomaly Detection");
        
        timeSeries
            .keyBy(TimeSeriesPoint::getMetric)
            .process(new AnomalyDetectionProcessor())
            .name("Statistical Anomaly Detection")
            .filter(alert -> alert != null)
            .print("Anomaly Alerts");
    }
    
    /**
     * Trend analysis and short-term forecasting
     */
    private static void runTrendAnalysis(DataStream<TimeSeriesPoint> timeSeries) {
        LOG.info("📊 Running Trend Analysis and Forecasting");
        
        timeSeries
            .keyBy(TimeSeriesPoint::getMetric)
            .window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(5)))
            .process(new TrendAnalysisProcessor())
            .name("Trend Analysis")
            .print("Trend Forecasts");
    }
    
    /**
     * Seasonal decomposition analysis
     */
    private static void runSeasonalDecomposition(DataStream<TimeSeriesPoint> timeSeries) {
        LOG.info("🌊 Running Seasonal Decomposition");
        
        timeSeries
            .keyBy(TimeSeriesPoint::getMetric)
            .window(TumblingEventTimeWindows.of(Time.hours(6)))
            .process(new SeasonalDecompositionProcessor())
            .name("Seasonal Decomposition")
            .print("Seasonal Components");
    }
    
    /**
     * Change point detection in time series
     */
    private static void runChangePointDetection(DataStream<TimeSeriesPoint> timeSeries) {
        LOG.info("🔄 Running Change Point Detection");
        
        timeSeries
            .keyBy(TimeSeriesPoint::getMetric)
            .process(new ChangePointDetectionProcessor())
            .name("Change Point Detection")
            .filter(changePoint -> changePoint != null)
            .print("Change Points");
    }
    
    /**
     * Statistical process control monitoring
     */
    private static void runStatisticalProcessControl(DataStream<TimeSeriesPoint> timeSeries) {
        LOG.info("📐 Running Statistical Process Control");
        
        timeSeries
            .keyBy(TimeSeriesPoint::getMetric)
            .window(SlidingEventTimeWindows.of(Time.minutes(20), Time.minutes(2)))
            .process(new StatisticalProcessControlProcessor())
            .name("SPC Monitoring")
            .print("SPC Alerts");
    }
    
    /**
     * Time series clustering and pattern recognition
     */
    private static void runTimeSeriesClustering(DataStream<TimeSeriesPoint> timeSeries) {
        LOG.info("🎯 Running Time Series Clustering");
        
        timeSeries
            .keyBy(TimeSeriesPoint::getMetric)
            .window(TumblingEventTimeWindows.of(Time.minutes(15)))
            .process(new TimeSeriesClusteringProcessor())
            .name("Time Series Clustering")
            .print("Pattern Clusters");
    }
    
    /**
     * Time series data point model
     */
    public static class TimeSeriesPoint {
        public String metric;
        public double value;
        public long timestamp;
        public Map<String, Double> features;
        public String source;
        
        public TimeSeriesPoint() {
            this.features = new HashMap<>();
        }
        
        public TimeSeriesPoint(String metric, double value, long timestamp, String source) {
            this.metric = metric;
            this.value = value;
            this.timestamp = timestamp;
            this.source = source;
            this.features = new HashMap<>();
        }
        
        public String getMetric() { return metric; }
        public double getValue() { return value; }
        public long getTimestamp() { return timestamp; }
        public String getSource() { return source; }
        
        @Override
        public String toString() {
            return String.format("TSPoint{metric='%s', value=%.3f, time=%d, source='%s'}", 
                               metric, value, timestamp, source);
        }
    }
    
    /**
     * Real-time anomaly detection using statistical methods
     */
    public static class AnomalyDetectionProcessor extends KeyedProcessFunction<String, TimeSeriesPoint, String> {
        
        private ValueState<MovingStatistics> statsState;
        private ValueState<Queue<Double>> windowState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            statsState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("movingStats", MovingStatistics.class));
            windowState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("window", Queue.class));
        }
        
        @Override
        public void processElement(TimeSeriesPoint point, Context ctx, Collector<String> out) throws Exception {
            MovingStatistics stats = statsState.value();
            Queue<Double> window = windowState.value();
            
            if (stats == null) {
                stats = new MovingStatistics(100); // Window size of 100
                window = new LinkedList<>();
            }
            
            // Update moving statistics
            stats.addValue(point.value);
            window.offer(point.value);
            
            // Maintain window size
            if (window.size() > 100) {
                window.poll();
            }
            
            // Detect anomalies using multiple methods
            if (stats.getCount() > 30) { // Need sufficient data
                List<String> anomalies = detectAnomalies(point, stats, new ArrayList<>(window));
                
                for (String anomaly : anomalies) {
                    out.collect(anomaly);
                }
            }
            
            // Update state
            statsState.update(stats);
            windowState.update(window);
        }
        
        private List<String> detectAnomalies(TimeSeriesPoint point, MovingStatistics stats, 
                                           List<Double> window) {
            List<String> anomalies = new ArrayList<>();
            
            // 1. Z-Score based detection
            double zScore = Math.abs((point.value - stats.getMean()) / stats.getStdDev());
            if (zScore > 3.0) {
                anomalies.add(String.format("🚨 Z-Score Anomaly in %s: value=%.3f, z-score=%.2f", 
                                          point.metric, point.value, zScore));
            }
            
            // 2. Interquartile Range (IQR) based detection
            Collections.sort(window);
            int q1Index = window.size() / 4;
            int q3Index = 3 * window.size() / 4;
            double q1 = window.get(q1Index);
            double q3 = window.get(q3Index);
            double iqr = q3 - q1;
            double lowerBound = q1 - 1.5 * iqr;
            double upperBound = q3 + 1.5 * iqr;
            
            if (point.value < lowerBound || point.value > upperBound) {
                anomalies.add(String.format("🚨 IQR Anomaly in %s: value=%.3f, bounds=[%.3f, %.3f]", 
                                          point.metric, point.value, lowerBound, upperBound));
            }
            
            // 3. Rate of change detection
            if (window.size() > 1) {
                double prevValue = window.get(window.size() - 2);
                double changeRate = Math.abs((point.value - prevValue) / prevValue);
                if (changeRate > 0.5) { // 50% change
                    anomalies.add(String.format("🚨 Rapid Change in %s: value=%.3f, change=%.1f%%", 
                                              point.metric, point.value, changeRate * 100));
                }
            }
            
            return anomalies;
        }
    }
    
    /**
     * Trend analysis and forecasting processor
     */
    public static class TrendAnalysisProcessor extends ProcessWindowFunction<TimeSeriesPoint, String, String, TimeWindow> {
        
        @Override
        public void process(String key, Context context, 
                          Iterable<TimeSeriesPoint> points, 
                          Collector<String> out) throws Exception {
            
            List<TimeSeriesPoint> pointList = new ArrayList<>();
            for (TimeSeriesPoint point : points) {
                pointList.add(point);
            }
            
            if (pointList.size() < 10) return;
            
            // Sort by timestamp
            pointList.sort(Comparator.comparing(TimeSeriesPoint::getTimestamp));
            
            // Calculate trend using linear regression
            TrendAnalysis trend = calculateTrend(pointList);
            
            // Generate short-term forecast
            double nextValue = forecast(pointList, 1);
            
            out.collect(String.format(
                "📊 Trend Analysis [%s]: slope=%.4f, R²=%.3f, direction=%s, forecast=%.3f", 
                key, trend.slope, trend.rSquared, trend.direction, nextValue
            ));
        }
        
        private TrendAnalysis calculateTrend(List<TimeSeriesPoint> points) {
            int n = points.size();
            double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0;
            
            for (int i = 0; i < n; i++) {
                double x = i; // Use index as x
                double y = points.get(i).value;
                
                sumX += x;
                sumY += y;
                sumXY += x * y;
                sumX2 += x * x;
                sumY2 += y * y;
            }
            
            double slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
            double intercept = (sumY - slope * sumX) / n;
            
            // Calculate R-squared
            double yMean = sumY / n;
            double ssRes = 0, ssTot = 0;
            for (int i = 0; i < n; i++) {
                double predicted = slope * i + intercept;
                double actual = points.get(i).value;
                ssRes += Math.pow(actual - predicted, 2);
                ssTot += Math.pow(actual - yMean, 2);
            }
            double rSquared = 1 - (ssRes / ssTot);
            
            String direction = slope > 0.01 ? "INCREASING" : 
                              slope < -0.01 ? "DECREASING" : "STABLE";
            
            return new TrendAnalysis(slope, intercept, rSquared, direction);
        }
        
        private double forecast(List<TimeSeriesPoint> points, int steps) {
            TrendAnalysis trend = calculateTrend(points);
            return trend.slope * (points.size() + steps - 1) + trend.intercept;
        }
    }
    
    /**
     * Seasonal decomposition processor
     */
    public static class SeasonalDecompositionProcessor extends ProcessWindowFunction<TimeSeriesPoint, String, String, TimeWindow> {
        
        @Override
        public void process(String key, Context context, 
                          Iterable<TimeSeriesPoint> points, 
                          Collector<String> out) throws Exception {
            
            List<Double> values = new ArrayList<>();
            for (TimeSeriesPoint point : points) {
                values.add(point.value);
            }
            
            if (values.size() < 24) return; // Need at least 24 points for hourly seasonality
            
            // Simple seasonal decomposition
            SeasonalComponents components = decomposeTimeSeries(values, 24); // 24-hour cycle
            
            out.collect(String.format(
                "🌊 Seasonal Decomposition [%s]: trend=%.3f, seasonal_strength=%.3f, residual_var=%.3f", 
                key, components.trend, components.seasonalStrength, components.residualVariance
            ));
        }
        
        private SeasonalComponents decomposeTimeSeries(List<Double> values, int period) {
            int n = values.size();
            
            // Calculate trend using moving average
            List<Double> trend = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                if (i < period/2 || i >= n - period/2) {
                    trend.add(0.0); // Padding
                } else {
                    double sum = 0;
                    for (int j = i - period/2; j <= i + period/2; j++) {
                        sum += values.get(j);
                    }
                    trend.add(sum / period);
                }
            }
            
            // Calculate seasonal component
            double[] seasonalPattern = new double[period];
            int[] counts = new int[period];
            
            for (int i = 0; i < n; i++) {
                if (trend.get(i) != 0.0) {
                    int seasonIndex = i % period;
                    seasonalPattern[seasonIndex] += values.get(i) - trend.get(i);
                    counts[seasonIndex]++;
                }
            }
            
            for (int i = 0; i < period; i++) {
                if (counts[i] > 0) {
                    seasonalPattern[i] /= counts[i];
                }
            }
            
            // Calculate residuals and metrics
            double seasonalVariance = 0;
            double residualVariance = 0;
            int validPoints = 0;
            
            for (int i = 0; i < n; i++) {
                if (trend.get(i) != 0.0) {
                    double seasonal = seasonalPattern[i % period];
                    double residual = values.get(i) - trend.get(i) - seasonal;
                    
                    seasonalVariance += seasonal * seasonal;
                    residualVariance += residual * residual;
                    validPoints++;
                }
            }
            
            if (validPoints > 0) {
                seasonalVariance /= validPoints;
                residualVariance /= validPoints;
            }
            
            double avgTrend = trend.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            double seasonalStrength = seasonalVariance / (seasonalVariance + residualVariance);
            
            return new SeasonalComponents(avgTrend, seasonalStrength, residualVariance);
        }
    }
    
    /**
     * Change point detection processor
     */
    public static class ChangePointDetectionProcessor extends KeyedProcessFunction<String, TimeSeriesPoint, String> {
        
        private ValueState<CircularBuffer> bufferState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            bufferState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("buffer", CircularBuffer.class));
        }
        
        @Override
        public void processElement(TimeSeriesPoint point, Context ctx, Collector<String> out) throws Exception {
            CircularBuffer buffer = bufferState.value();
            if (buffer == null) {
                buffer = new CircularBuffer(50); // Buffer size
            }
            
            buffer.add(point.value);
            
            if (buffer.isFull()) {
                // CUSUM change point detection
                double changePoint = detectChangePointCUSUM(buffer.getValues());
                if (changePoint > 0) {
                    out.collect(String.format(
                        "🔄 Change Point Detected in %s: score=%.3f, value=%.3f", 
                        point.metric, changePoint, point.value
                    ));
                }
            }
            
            bufferState.update(buffer);
        }
        
        private double detectChangePointCUSUM(List<Double> values) {
            int n = values.size();
            if (n < 20) return 0;
            
            // Split data into two parts and compare means
            int midPoint = n / 2;
            
            double sum1 = 0, sum2 = 0;
            for (int i = 0; i < midPoint; i++) {
                sum1 += values.get(i);
            }
            for (int i = midPoint; i < n; i++) {
                sum2 += values.get(i);
            }
            
            double mean1 = sum1 / midPoint;
            double mean2 = sum2 / (n - midPoint);
            
            // Calculate pooled standard deviation
            double variance1 = 0, variance2 = 0;
            for (int i = 0; i < midPoint; i++) {
                variance1 += Math.pow(values.get(i) - mean1, 2);
            }
            for (int i = midPoint; i < n; i++) {
                variance2 += Math.pow(values.get(i) - mean2, 2);
            }
            
            double pooledVar = (variance1 + variance2) / (n - 2);
            double pooledStd = Math.sqrt(pooledVar);
            
            // CUSUM statistic
            if (pooledStd > 0) {
                return Math.abs(mean2 - mean1) / pooledStd;
            }
            
            return 0;
        }
    }
    
    /**
     * Statistical Process Control processor
     */
    public static class StatisticalProcessControlProcessor extends ProcessWindowFunction<TimeSeriesPoint, String, String, TimeWindow> {
        
        @Override
        public void process(String key, Context context, 
                          Iterable<TimeSeriesPoint> points, 
                          Collector<String> out) throws Exception {
            
            List<Double> values = new ArrayList<>();
            for (TimeSeriesPoint point : points) {
                values.add(point.value);
            }
            
            if (values.size() < 10) return;
            
            // Calculate control limits
            DescriptiveStatistics stats = new DescriptiveStatistics();
            values.forEach(stats::addValue);
            
            double mean = stats.getMean();
            double stdDev = stats.getStandardDeviation();
            
            double ucl = mean + 3 * stdDev; // Upper Control Limit
            double lcl = mean - 3 * stdDev; // Lower Control Limit
            double uwl = mean + 2 * stdDev; // Upper Warning Limit
            double lwl = mean - 2 * stdDev; // Lower Warning Limit
            
            // Check for out-of-control conditions
            List<String> alerts = new ArrayList<>();
            
            for (int i = 0; i < values.size(); i++) {
                double value = values.get(i);
                
                if (value > ucl || value < lcl) {
                    alerts.add("OUT_OF_CONTROL");
                } else if (value > uwl || value < lwl) {
                    alerts.add("WARNING");
                }
            }
            
            // Check for trends (7 consecutive points above/below mean)
            int consecutiveAbove = 0, consecutiveBelow = 0;
            for (double value : values) {
                if (value > mean) {
                    consecutiveAbove++;
                    consecutiveBelow = 0;
                } else {
                    consecutiveBelow++;
                    consecutiveAbove = 0;
                }
                
                if (consecutiveAbove >= 7 || consecutiveBelow >= 7) {
                    alerts.add("TREND_DETECTED");
                    break;
                }
            }
            
            if (!alerts.isEmpty()) {
                out.collect(String.format(
                    "📐 SPC Alert [%s]: %s, UCL=%.3f, LCL=%.3f, mean=%.3f", 
                    key, String.join(", ", alerts), ucl, lcl, mean
                ));
            }
        }
    }
    
    /**
     * Time series clustering processor
     */
    public static class TimeSeriesClusteringProcessor extends ProcessWindowFunction<TimeSeriesPoint, String, String, TimeWindow> {
        
        @Override
        public void process(String key, Context context, 
                          Iterable<TimeSeriesPoint> points, 
                          Collector<String> out) throws Exception {
            
            List<TimeSeriesPoint> pointList = new ArrayList<>();
            for (TimeSeriesPoint point : points) {
                pointList.add(point);
            }
            
            if (pointList.size() < 20) return;
            
            // Extract features for clustering
            TimeSeriesFeatures features = extractFeatures(pointList);
            
            // Simple pattern classification based on features
            String pattern = classifyPattern(features);
            
            out.collect(String.format(
                "🎯 Pattern Classification [%s]: pattern=%s, features=%s", 
                key, pattern, features
            ));
        }
        
        private TimeSeriesFeatures extractFeatures(List<TimeSeriesPoint> points) {
            DescriptiveStatistics stats = new DescriptiveStatistics();
            points.stream().mapToDouble(TimeSeriesPoint::getValue).forEach(stats::addValue);
            
            double mean = stats.getMean();
            double variance = stats.getVariance();
            double skewness = stats.getSkewness();
            double kurtosis = stats.getKurtosis();
            
            // Calculate autocorrelation at lag 1
            double autocorr = calculateAutocorrelation(points, 1);
            
            // Calculate trend strength
            double trendStrength = calculateTrendStrength(points);
            
            return new TimeSeriesFeatures(mean, variance, skewness, kurtosis, autocorr, trendStrength);
        }
        
        private double calculateAutocorrelation(List<TimeSeriesPoint> points, int lag) {
            if (points.size() <= lag) return 0;
            
            List<Double> values = points.stream().mapToDouble(TimeSeriesPoint::getValue)
                                        .boxed().collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
            
            double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            
            double numerator = 0, denominator = 0;
            
            for (int i = 0; i < values.size() - lag; i++) {
                numerator += (values.get(i) - mean) * (values.get(i + lag) - mean);
            }
            
            for (double value : values) {
                denominator += Math.pow(value - mean, 2);
            }
            
            return denominator > 0 ? numerator / denominator : 0;
        }
        
        private double calculateTrendStrength(List<TimeSeriesPoint> points) {
            if (points.size() < 3) return 0;
            
            // Linear regression slope as trend strength
            int n = points.size();
            double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
            
            for (int i = 0; i < n; i++) {
                double x = i;
                double y = points.get(i).value;
                
                sumX += x;
                sumY += y;
                sumXY += x * y;
                sumX2 += x * x;
            }
            
            return (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        }
        
        private String classifyPattern(TimeSeriesFeatures features) {
            if (Math.abs(features.trendStrength) > 0.1) {
                return features.trendStrength > 0 ? "INCREASING_TREND" : "DECREASING_TREND";
            } else if (features.variance > features.mean * features.mean) {
                return "HIGH_VOLATILITY";
            } else if (Math.abs(features.autocorr) > 0.7) {
                return "CYCLICAL";
            } else if (Math.abs(features.skewness) > 1.0) {
                return "SKEWED";
            } else {
                return "STATIONARY";
            }
        }
    }
    
    // Helper classes
    public static class MovingStatistics {
        private final DescriptiveStatistics stats;
        
        public MovingStatistics(int windowSize) {
            this.stats = new DescriptiveStatistics(windowSize);
        }
        
        public void addValue(double value) {
            stats.addValue(value);
        }
        
        public double getMean() { return stats.getMean(); }
        public double getStdDev() { return stats.getStandardDeviation(); }
        public long getCount() { return stats.getN(); }
    }
    
    public static class TrendAnalysis {
        public final double slope;
        public final double intercept;
        public final double rSquared;
        public final String direction;
        
        public TrendAnalysis(double slope, double intercept, double rSquared, String direction) {
            this.slope = slope;
            this.intercept = intercept;
            this.rSquared = rSquared;
            this.direction = direction;
        }
    }
    
    public static class SeasonalComponents {
        public final double trend;
        public final double seasonalStrength;
        public final double residualVariance;
        
        public SeasonalComponents(double trend, double seasonalStrength, double residualVariance) {
            this.trend = trend;
            this.seasonalStrength = seasonalStrength;
            this.residualVariance = residualVariance;
        }
    }
    
    public static class CircularBuffer {
        private final double[] buffer;
        private int head = 0;
        private int size = 0;
        private final int capacity;
        
        public CircularBuffer(int capacity) {
            this.capacity = capacity;
            this.buffer = new double[capacity];
        }
        
        public void add(double value) {
            buffer[head] = value;
            head = (head + 1) % capacity;
            if (size < capacity) size++;
        }
        
        public boolean isFull() {
            return size == capacity;
        }
        
        public List<Double> getValues() {
            List<Double> values = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                int index = (head - size + i + capacity) % capacity;
                values.add(buffer[index]);
            }
            return values;
        }
    }
    
    public static class TimeSeriesFeatures {
        public final double mean, variance, skewness, kurtosis, autocorr, trendStrength;
        
        public TimeSeriesFeatures(double mean, double variance, double skewness, 
                                double kurtosis, double autocorr, double trendStrength) {
            this.mean = mean;
            this.variance = variance;
            this.skewness = skewness;
            this.kurtosis = kurtosis;
            this.autocorr = autocorr;
            this.trendStrength = trendStrength;
        }
        
        @Override
        public String toString() {
            return String.format("Features{mean=%.2f, var=%.2f, skew=%.2f, kurt=%.2f, autocorr=%.2f, trend=%.2f}", 
                               mean, variance, skewness, kurtosis, autocorr, trendStrength);
        }
    }
    
    /**
     * Time series data generator
     */
    public static class TimeSeriesDataGenerator extends org.apache.flink.streaming.api.functions.source.SourceFunction<TimeSeriesPoint> {
        private volatile boolean running = true;
        private final Random random = new Random();
        private final String[] metrics = {"cpu_usage", "memory_usage", "network_traffic", "disk_io", "temperature"};
        private final Map<String, Double> baselines = new HashMap<>();
        
        public TimeSeriesDataGenerator() {
            // Initialize baselines for different metrics
            baselines.put("cpu_usage", 45.0);
            baselines.put("memory_usage", 60.0);
            baselines.put("network_traffic", 100.0);
            baselines.put("disk_io", 75.0);
            baselines.put("temperature", 25.0);
        }
        
        @Override
        public void run(SourceContext<TimeSeriesPoint> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            int counter = 0;
            
            while (running) {
                for (String metric : metrics) {
                    double value = generateValue(metric, counter);
                    long timestamp = startTime + counter * 1000; // 1 second intervals
                    
                    TimeSeriesPoint point = new TimeSeriesPoint(metric, value, timestamp, "system");
                    
                    // Add some contextual features
                    point.features.put("hour_of_day", (double) (counter % 24));
                    point.features.put("day_of_week", (double) ((counter / 24) % 7));
                    
                    ctx.collect(point);
                }
                
                counter++;
                Thread.sleep(1000); // 1 second intervals
            }
        }
        
        private double generateValue(String metric, int timeStep) {
            double baseline = baselines.get(metric);
            double noise = random.nextGaussian() * 5; // Random noise
            
            // Add seasonal patterns
            double hourlyPattern = 10 * Math.sin(2 * Math.PI * timeStep / 24.0); // Daily cycle
            double weeklyPattern = 5 * Math.sin(2 * Math.PI * timeStep / (24.0 * 7)); // Weekly cycle
            
            // Add trend
            double trend = 0.01 * timeStep; // Slight upward trend
            
            // Occasional anomalies
            double anomaly = 0;
            if (random.nextDouble() < 0.05) { // 5% chance
                anomaly = random.nextGaussian() * 20;
            }
            
            double value = baseline + hourlyPattern + weeklyPattern + trend + noise + anomaly;
            
            // Ensure positive values for some metrics
            if (metric.equals("cpu_usage") || metric.equals("memory_usage")) {
                value = Math.max(0, Math.min(100, value)); // 0-100 range
            } else if (metric.equals("temperature")) {
                value = Math.max(-10, Math.min(50, value)); // Temperature range
            } else {
                value = Math.max(0, value); // Non-negative
            }
            
            return value;
        }
        
        @Override
        public void cancel() {
            running = false;
        }
    }
}
