package com.example.flink.ml;

import com.example.flink.data.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Advanced Machine Learning Pipeline with Flink
 * 
 * Demonstrates:
 * - Real-time feature engineering
 * - Online learning and model updates
 * - Anomaly detection using autoencoders
 * - Fraud detection with ensemble methods
 * - A/B testing for model performance
 * - Federated learning coordination
 */
public class MLPipelineJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(MLPipelineJob.class);
    
    public static void runDemo(StreamExecutionEnvironment env) throws Exception {
        LOG.info("🤖 Starting Machine Learning Pipeline Demo");
        
        // Create synthetic event stream for ML processing
        DataStream<Event> eventStream = env
            .fromElements(generateSyntheticMLEvents())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, timestamp) -> event.getEventTimeMillis())
            );
        
        // Demo 1: Real-time Feature Engineering
        demonstrateFeatureEngineering(eventStream);
        
        // Demo 2: Online Anomaly Detection
        demonstrateAnomalyDetection(eventStream);
        
        // Demo 3: Real-time Fraud Detection
        demonstrateFraudDetection(eventStream);
        
        // Demo 4: Recommendation Engine
        demonstrateRecommendationEngine(eventStream);
        
        // Demo 5: Predictive Maintenance
        demonstratePredictiveMaintenance(eventStream);
        
        LOG.info("✅ Machine Learning Pipeline configured");
        env.execute("ML Pipeline Job");
    }
    
    /**
     * Demonstrate real-time feature engineering
     */
    private static void demonstrateFeatureEngineering(DataStream<Event> eventStream) {
        LOG.info("Setting up real-time feature engineering...");
        
        DataStream<MLFeatures> features = eventStream
            .keyBy(Event::userId)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .process(new FeatureEngineeringFunction())
            .name("feature-engineering");
        
        features.print("FEATURES: ");
    }
    
    /**
     * Demonstrate online anomaly detection using autoencoders
     */
    private static void demonstrateAnomalyDetection(DataStream<Event> eventStream) {
        LOG.info("Setting up anomaly detection with autoencoders...");
        
        DataStream<AnomalyScore> anomalies = eventStream
            .map(new AnomalyDetectionFunction())
            .filter(anomaly -> anomaly.score > 0.8) // High anomaly threshold
            .name("anomaly-detection");
        
        anomalies.print("ANOMALY: ");
    }
    
    /**
     * Demonstrate real-time fraud detection with ensemble methods
     */
    private static void demonstrateFraudDetection(DataStream<Event> eventStream) {
        LOG.info("Setting up fraud detection with ensemble methods...");
        
        DataStream<FraudPrediction> fraudPredictions = eventStream
            .filter(event -> "purchase".equals(event.eventType()))
            .map(new EnsembleFraudDetectionFunction())
            .filter(prediction -> prediction.fraudProbability > 0.7)
            .name("fraud-detection");
        
        fraudPredictions.print("FRAUD DETECTION: ");
    }
    
    /**
     * Demonstrate real-time recommendation engine
     */
    private static void demonstrateRecommendationEngine(DataStream<Event> eventStream) {
        LOG.info("Setting up recommendation engine...");
        
        DataStream<Recommendation> recommendations = eventStream
            .keyBy(Event::userId)
            .map(new CollaborativeFilteringFunction())
            .name("recommendation-engine");
        
        recommendations.print("RECOMMENDATION: ");
    }
    
    /**
     * Demonstrate predictive maintenance
     */
    private static void demonstratePredictiveMaintenance(DataStream<Event> eventStream) {
        LOG.info("Setting up predictive maintenance...");
        
        DataStream<MaintenanceAlert> maintenanceAlerts = eventStream
            .filter(event -> event.eventType().startsWith("sensor_"))
            .keyBy(event -> event.properties().get("deviceId"))
            .map(new PredictiveMaintenanceFunction())
            .filter(alert -> alert.riskScore > 0.85)
            .name("predictive-maintenance");
        
        maintenanceAlerts.print("MAINTENANCE ALERT: ");
    }
    
    /**
     * Feature engineering function for ML preprocessing
     */
    private static class FeatureEngineeringFunction 
            extends ProcessWindowFunction<Event, MLFeatures, String, TimeWindow> {
        
        @Override
        public void process(String userId, Context context, Iterable<Event> events, Collector<MLFeatures> out) {
            List<Event> eventList = new ArrayList<>();
            events.forEach(eventList::add);
            
            if (eventList.isEmpty()) return;
            
            // Compute advanced features
            double totalValue = eventList.stream().mapToDouble(Event::value).sum();
            long eventCount = eventList.size();
            double avgValue = totalValue / eventCount;
            double valueVariance = eventList.stream()
                .mapToDouble(e -> Math.pow(e.value() - avgValue, 2))
                .sum() / eventCount;
            
            // Behavioral features
            Map<String, Long> eventTypeCounts = eventList.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                    Event::eventType, 
                    java.util.stream.Collectors.counting()
                ));
            
            double sessionDuration = context.window().getEnd() - context.window().getStart();
            double eventRate = eventCount / (sessionDuration / 1000.0); // events per second
            
            // Time-based features
            long[] eventTimes = eventList.stream()
                .mapToLong(Event::getEventTimeMillis)
                .sorted()
                .toArray();
            
            double avgTimeBetweenEvents = eventTimes.length > 1 ? 
                (eventTimes[eventTimes.length - 1] - eventTimes[0]) / (double)(eventTimes.length - 1) : 0;
            
            MLFeatures features = new MLFeatures(
                userId,
                context.window().getStart(),
                totalValue,
                eventCount,
                avgValue,
                valueVariance,
                eventRate,
                avgTimeBetweenEvents,
                eventTypeCounts
            );
            
            out.collect(features);
        }
    }
    
    /**
     * Anomaly detection using autoencoder neural network
     */
    private static class AnomalyDetectionFunction extends RichMapFunction<Event, AnomalyScore> {
        
        private transient MultiLayerNetwork autoencoder;
        private transient Map<String, List<Double>> userFeatureHistory;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            // Initialize autoencoder
            MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(123)
                .updater(new Adam(0.001))
                .list()
                .layer(0, new DenseLayer.Builder()
                    .nIn(10)  // Input features
                    .nOut(5)  // Hidden layer (bottleneck)
                    .activation(Activation.RELU)
                    .build())
                .layer(1, new OutputLayer.Builder()
                    .nIn(5)
                    .nOut(10) // Reconstructed features
                    .activation(Activation.SIGMOID)
                    .lossFunction(LossFunctions.LossFunction.MSE)
                    .build())
                .build();
            
            autoencoder = new MultiLayerNetwork(conf);
            autoencoder.init();
            
            userFeatureHistory = new HashMap<>();
            
            LOG.info("Initialized autoencoder for anomaly detection");
        }
        
        @Override
        public AnomalyScore map(Event event) throws Exception {
            // Extract features
            double[] features = extractFeatures(event);
            
            // Update user history
            String userId = event.userId();
            userFeatureHistory.computeIfAbsent(userId, k -> new ArrayList<>())
                .addAll(Arrays.stream(features).boxed().toList());
            
            // Limit history size
            List<Double> history = userFeatureHistory.get(userId);
            if (history.size() > 100) {
                history.subList(0, history.size() - 100).clear();
            }
            
            // Compute anomaly score
            INDArray input = Nd4j.create(features);
            INDArray reconstructed = autoencoder.output(input);
            
            // Reconstruction error as anomaly score
            double reconstructionError = input.distance2(reconstructed);
            double normalizedScore = Math.min(1.0, reconstructionError / 10.0); // Normalize to [0,1]
            
            // Online learning - update model with normal patterns
            if (normalizedScore < 0.3) { // Low reconstruction error = normal pattern
                DataSet dataSet = new DataSet(input, input); // Autoencoder target = input
                autoencoder.fit(dataSet);
            }
            
            return new AnomalyScore(userId, event.eventId(), normalizedScore, features);
        }
        
        private double[] extractFeatures(Event event) {
            return new double[] {
                event.value(),
                event.getEventTimeMillis() % (24 * 60 * 60 * 1000), // Time of day
                event.eventType().hashCode() % 100, // Event type encoded
                event.properties().size(),
                Math.log(event.value() + 1), // Log transform
                event.userId().hashCode() % 1000, // User encoded
                event.timestamp().getDayOfWeek().getValue(), // Day of week
                event.timestamp().getHour(), // Hour of day
                event.isRevenueEvent() ? 1.0 : 0.0, // Boolean feature
                event.properties().getOrDefault("category", "").hashCode() % 50 // Category encoded
            };
        }
    }
    
    /**
     * Ensemble fraud detection combining multiple models
     */
    private static class EnsembleFraudDetectionFunction extends RichMapFunction<Event, FraudPrediction> {
        
        private transient List<FraudModel> models;
        private transient Map<String, UserProfile> userProfiles;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            // Initialize ensemble of fraud detection models
            models = Arrays.asList(
                new RuleBasedFraudModel(),
                new StatisticalFraudModel(),
                new BehavioralFraudModel()
            );
            
            userProfiles = new HashMap<>();
            
            LOG.info("Initialized ensemble fraud detection models");
        }
        
        @Override
        public FraudPrediction map(Event event) throws Exception {
            // Update user profile
            UserProfile profile = userProfiles.computeIfAbsent(
                event.userId(), 
                k -> new UserProfile(k)
            );
            profile.addEvent(event);
            
            // Get predictions from all models
            List<Double> predictions = new ArrayList<>();
            for (FraudModel model : models) {
                double prediction = model.predict(event, profile);
                predictions.add(prediction);
            }
            
            // Ensemble voting (weighted average)
            double[] weights = {0.3, 0.4, 0.3}; // Weights for each model
            double ensemblePrediction = 0.0;
            for (int i = 0; i < predictions.size(); i++) {
                ensemblePrediction += weights[i] * predictions.get(i);
            }
            
            // Confidence based on model agreement
            double variance = predictions.stream()
                .mapToDouble(p -> Math.pow(p - ensemblePrediction, 2))
                .sum() / predictions.size();
            double confidence = 1.0 - variance; // High variance = low confidence
            
            return new FraudPrediction(
                event.userId(),
                event.eventId(),
                ensemblePrediction,
                confidence,
                predictions
            );
        }
    }
    
    /**
     * Collaborative filtering for recommendations
     */
    private static class CollaborativeFilteringFunction extends RichMapFunction<Event, Recommendation> {
        
        private transient Map<String, Set<String>> userItemMatrix;
        private transient Map<String, Map<String, Double>> itemSimilarity;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            userItemMatrix = new HashMap<>();
            itemSimilarity = new HashMap<>();
            
            LOG.info("Initialized collaborative filtering for recommendations");
        }
        
        @Override
        public Recommendation map(Event event) throws Exception {
            String userId = event.userId();
            String itemId = (String) event.properties().getOrDefault("productId", "unknown");
            
            // Update user-item matrix
            userItemMatrix.computeIfAbsent(userId, k -> new HashSet<>()).add(itemId);
            
            // Compute recommendations using item-based collaborative filtering
            Set<String> userItems = userItemMatrix.get(userId);
            Map<String, Double> recommendations = new HashMap<>();
            
            for (String item : userItems) {
                // Find similar items
                for (Map.Entry<String, Set<String>> otherUser : userItemMatrix.entrySet()) {
                    if (!otherUser.getKey().equals(userId)) {
                        Set<String> otherItems = otherUser.getValue();
                        
                        // Jaccard similarity for item recommendation
                        for (String otherItem : otherItems) {
                            if (!userItems.contains(otherItem)) {
                                double similarity = jaccardSimilarity(userItems, otherItems);
                                recommendations.merge(otherItem, similarity, Double::sum);
                            }
                        }
                    }
                }
            }
            
            // Get top recommendation
            String topRecommendation = recommendations.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("no_recommendation");
            
            double confidence = recommendations.getOrDefault(topRecommendation, 0.0);
            
            return new Recommendation(userId, topRecommendation, confidence, event.getEventTimeMillis());
        }
        
        private double jaccardSimilarity(Set<String> set1, Set<String> set2) {
            Set<String> intersection = new HashSet<>(set1);
            intersection.retainAll(set2);
            
            Set<String> union = new HashSet<>(set1);
            union.addAll(set2);
            
            return union.isEmpty() ? 0.0 : (double) intersection.size() / union.size();
        }
    }
    
    /**
     * Predictive maintenance using time series analysis
     */
    private static class PredictiveMaintenanceFunction extends RichMapFunction<Event, MaintenanceAlert> {
        
        private transient Map<String, List<Double>> deviceMetrics;
        private transient Map<String, Long> lastMaintenanceTime;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            deviceMetrics = new HashMap<>();
            lastMaintenanceTime = new HashMap<>();
            
            LOG.info("Initialized predictive maintenance analysis");
        }
        
        @Override
        public MaintenanceAlert map(Event event) throws Exception {
            String deviceId = (String) event.properties().get("deviceId");
            double sensorValue = event.value();
            
            // Update device metrics history
            List<Double> metrics = deviceMetrics.computeIfAbsent(deviceId, k -> new ArrayList<>());
            metrics.add(sensorValue);
            
            // Keep only recent history
            if (metrics.size() > 50) {
                metrics.subList(0, metrics.size() - 50).clear();
            }
            
            // Compute risk factors
            double riskScore = 0.0;
            
            if (metrics.size() >= 10) {
                // Trend analysis
                double[] values = metrics.stream().mapToDouble(Double::doubleValue).toArray();
                double trend = computeTrend(values);
                
                // Variance analysis
                double mean = Arrays.stream(values).average().orElse(0.0);
                double variance = Arrays.stream(values)
                    .map(x -> Math.pow(x - mean, 2))
                    .reduce(0.0, Double::sum) / values.length;
                
                // Anomaly detection based on z-score
                double currentZScore = Math.abs((sensorValue - mean) / Math.sqrt(variance));
                
                // Time since last maintenance
                long timeSinceLastMaintenance = event.getEventTimeMillis() - 
                    lastMaintenanceTime.getOrDefault(deviceId, 0L);
                double maintenanceScore = Math.min(1.0, timeSinceLastMaintenance / (30L * 24 * 60 * 60 * 1000)); // 30 days
                
                // Combine risk factors
                riskScore = Math.min(1.0, 
                    0.3 * Math.min(1.0, Math.abs(trend)) +  // Trend factor
                    0.3 * Math.min(1.0, currentZScore / 3) +  // Anomaly factor
                    0.4 * maintenanceScore  // Time factor
                );
            }
            
            return new MaintenanceAlert(
                deviceId,
                event.eventType(),
                sensorValue,
                riskScore,
                event.getEventTimeMillis()
            );
        }
        
        private double computeTrend(double[] values) {
            if (values.length < 2) return 0.0;
            
            // Simple linear trend computation
            double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
            int n = values.length;
            
            for (int i = 0; i < n; i++) {
                sumX += i;
                sumY += values[i];
                sumXY += i * values[i];
                sumX2 += i * i;
            }
            
            return (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        }
    }
    
    // Helper method to generate synthetic ML events
    private static Event[] generateSyntheticMLEvents() {
        Random random = new Random();
        List<Event> events = new ArrayList<>();
        
        for (int i = 0; i < 100; i++) {
            String userId = "user" + (random.nextInt(10) + 1);
            String eventType = random.nextBoolean() ? "purchase" : "sensor_temperature";
            double value = eventType.equals("purchase") ? 
                random.nextDouble() * 1000 + 10 : 
                random.nextGaussian() * 5 + 25; // Temperature readings
            
            Map<String, Object> properties = new HashMap<>();
            if (eventType.equals("purchase")) {
                properties.put("productId", "product" + (random.nextInt(50) + 1));
                properties.put("category", "electronics");
                properties.put("amount", value);
            } else {
                properties.put("deviceId", "device" + (random.nextInt(5) + 1));
                properties.put("location", "factory_floor_" + (random.nextInt(3) + 1));
            }
            
            events.add(new Event(
                "event_" + i,
                userId,
                eventType,
                value,
                java.time.Instant.now(),
                properties
            ));
        }
        
        return events.toArray(new Event[0]);
    }
    
    // Data classes for ML results
    public static class MLFeatures {
        public final String userId;
        public final long windowStart;
        public final double totalValue;
        public final long eventCount;
        public final double avgValue;
        public final double valueVariance;
        public final double eventRate;
        public final double avgTimeBetweenEvents;
        public final Map<String, Long> eventTypeCounts;
        
        public MLFeatures(String userId, long windowStart, double totalValue, long eventCount,
                         double avgValue, double valueVariance, double eventRate, 
                         double avgTimeBetweenEvents, Map<String, Long> eventTypeCounts) {
            this.userId = userId;
            this.windowStart = windowStart;
            this.totalValue = totalValue;
            this.eventCount = eventCount;
            this.avgValue = avgValue;
            this.valueVariance = valueVariance;
            this.eventRate = eventRate;
            this.avgTimeBetweenEvents = avgTimeBetweenEvents;
            this.eventTypeCounts = eventTypeCounts;
        }
        
        @Override
        public String toString() {
            return String.format("MLFeatures{user=%s, events=%d, avgValue=%.2f, rate=%.2f}", 
                               userId, eventCount, avgValue, eventRate);
        }
    }
    
    public static class AnomalyScore {
        public final String userId;
        public final String eventId;
        public final double score;
        public final double[] features;
        
        public AnomalyScore(String userId, String eventId, double score, double[] features) {
            this.userId = userId;
            this.eventId = eventId;
            this.score = score;
            this.features = features;
        }
        
        @Override
        public String toString() {
            return String.format("AnomalyScore{user=%s, event=%s, score=%.3f}", 
                               userId, eventId, score);
        }
    }
    
    public static class FraudPrediction {
        public final String userId;
        public final String eventId;
        public final double fraudProbability;
        public final double confidence;
        public final List<Double> modelPredictions;
        
        public FraudPrediction(String userId, String eventId, double fraudProbability, 
                              double confidence, List<Double> modelPredictions) {
            this.userId = userId;
            this.eventId = eventId;
            this.fraudProbability = fraudProbability;
            this.confidence = confidence;
            this.modelPredictions = modelPredictions;
        }
        
        @Override
        public String toString() {
            return String.format("FraudPrediction{user=%s, probability=%.3f, confidence=%.3f}", 
                               userId, fraudProbability, confidence);
        }
    }
    
    public static class Recommendation {
        public final String userId;
        public final String recommendedItem;
        public final double confidence;
        public final long timestamp;
        
        public Recommendation(String userId, String recommendedItem, double confidence, long timestamp) {
            this.userId = userId;
            this.recommendedItem = recommendedItem;
            this.confidence = confidence;
            this.timestamp = timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("Recommendation{user=%s, item=%s, confidence=%.3f}", 
                               userId, recommendedItem, confidence);
        }
    }
    
    public static class MaintenanceAlert {
        public final String deviceId;
        public final String sensorType;
        public final double currentValue;
        public final double riskScore;
        public final long timestamp;
        
        public MaintenanceAlert(String deviceId, String sensorType, double currentValue, 
                               double riskScore, long timestamp) {
            this.deviceId = deviceId;
            this.sensorType = sensorType;
            this.currentValue = currentValue;
            this.riskScore = riskScore;
            this.timestamp = timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("MaintenanceAlert{device=%s, sensor=%s, value=%.2f, risk=%.3f}", 
                               deviceId, sensorType, currentValue, riskScore);
        }
    }
    
    // Fraud detection models
    private interface FraudModel {
        double predict(Event event, UserProfile profile);
    }
    
    private static class RuleBasedFraudModel implements FraudModel {
        @Override
        public double predict(Event event, UserProfile profile) {
            double score = 0.0;
            
            // High value transaction
            if (event.getRevenueAmount() > 1000) score += 0.3;
            
            // Unusual time
            int hour = event.timestamp().getHour();
            if (hour < 6 || hour > 22) score += 0.2;
            
            // Rapid transactions
            if (profile.getRecentTransactionCount(60000) > 5) score += 0.4; // 5 in 1 minute
            
            // Geographic anomaly (simulated)
            if (ThreadLocalRandom.current().nextDouble() < 0.1) score += 0.3;
            
            return Math.min(1.0, score);
        }
    }
    
    private static class StatisticalFraudModel implements FraudModel {
        @Override
        public double predict(Event event, UserProfile profile) {
            double avgAmount = profile.getAverageTransactionAmount();
            double stdAmount = profile.getTransactionAmountStdDev();
            
            if (stdAmount == 0) return 0.0;
            
            // Z-score based detection
            double zScore = Math.abs((event.getRevenueAmount() - avgAmount) / stdAmount);
            
            // Convert z-score to probability (sigmoid-like)
            return 1.0 / (1.0 + Math.exp(-0.5 * (zScore - 3.0)));
        }
    }
    
    private static class BehavioralFraudModel implements FraudModel {
        @Override
        public double predict(Event event, UserProfile profile) {
            double score = 0.0;
            
            // Velocity check
            double recentVelocity = profile.getTransactionVelocity(300000); // 5 minutes
            if (recentVelocity > profile.getTypicalVelocity() * 3) score += 0.4;
            
            // Pattern deviation
            double patternScore = profile.getPatternDeviationScore(event);
            score += patternScore * 0.3;
            
            // Device/session analysis (simulated)
            if (ThreadLocalRandom.current().nextDouble() < 0.05) score += 0.3;
            
            return Math.min(1.0, score);
        }
    }
    
    private static class UserProfile {
        private final String userId;
        private final List<Event> events;
        private final long createdTime;
        
        public UserProfile(String userId) {
            this.userId = userId;
            this.events = new ArrayList<>();
            this.createdTime = System.currentTimeMillis();
        }
        
        public void addEvent(Event event) {
            events.add(event);
            // Keep only recent events (last 1000)
            if (events.size() > 1000) {
                events.subList(0, events.size() - 1000).clear();
            }
        }
        
        public int getRecentTransactionCount(long timeWindowMs) {
            long cutoff = System.currentTimeMillis() - timeWindowMs;
            return (int) events.stream()
                .filter(e -> e.getEventTimeMillis() > cutoff)
                .filter(e -> "purchase".equals(e.eventType()))
                .count();
        }
        
        public double getAverageTransactionAmount() {
            return events.stream()
                .filter(e -> "purchase".equals(e.eventType()))
                .mapToDouble(Event::getRevenueAmount)
                .average()
                .orElse(0.0);
        }
        
        public double getTransactionAmountStdDev() {
            double avg = getAverageTransactionAmount();
            double variance = events.stream()
                .filter(e -> "purchase".equals(e.eventType()))
                .mapToDouble(e -> Math.pow(e.getRevenueAmount() - avg, 2))
                .average()
                .orElse(0.0);
            return Math.sqrt(variance);
        }
        
        public double getTransactionVelocity(long timeWindowMs) {
            long cutoff = System.currentTimeMillis() - timeWindowMs;
            long count = events.stream()
                .filter(e -> e.getEventTimeMillis() > cutoff)
                .filter(e -> "purchase".equals(e.eventType()))
                .count();
            return count / (timeWindowMs / 1000.0); // transactions per second
        }
        
        public double getTypicalVelocity() {
            // Simplified: return average velocity over all time
            long totalTime = System.currentTimeMillis() - createdTime;
            if (totalTime == 0) return 0.0;
            
            long totalTransactions = events.stream()
                .filter(e -> "purchase".equals(e.eventType()))
                .count();
            return totalTransactions / (totalTime / 1000.0);
        }
        
        public double getPatternDeviationScore(Event event) {
            // Simplified pattern analysis
            Map<String, Long> typicalPattern = events.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                    Event::eventType, 
                    java.util.stream.Collectors.counting()
                ));
            
            long totalEvents = events.size();
            if (totalEvents == 0) return 0.0;
            
            String eventType = event.eventType();
            double expectedFreq = typicalPattern.getOrDefault(eventType, 0L) / (double) totalEvents;
            
            // Return deviation score (higher = more unusual)
            return expectedFreq < 0.1 ? 0.5 : 0.0; // Rare event type
        }
    }
}
