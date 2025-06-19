package com.example.flink.federated;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Advanced Federated Learning with Apache Flink
 * 
 * Demonstrates:
 * - Distributed model training coordination
 * - Secure aggregation protocols
 * - Privacy-preserving learning
 * - Cross-silo and cross-device federation
 * - Adaptive federated optimization
 * - Model versioning and rollback
 * - Byzantine fault tolerance
 * - Differential privacy mechanisms
 */
public class FederatedLearningJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(FederatedLearningJob.class);
    
    public static void runDemo(StreamExecutionEnvironment env) throws Exception {
        LOG.info("🔗 Starting Advanced Federated Learning Demo");
        
        // Configure watermark strategy
        WatermarkStrategy<FederatedUpdate> watermarkStrategy = WatermarkStrategy
            .<FederatedUpdate>forBoundedOutOfOrderness(Duration.ofSeconds(10))
            .withTimestampAssigner((update, timestamp) -> update.timestamp);
        
        // Generate federated learning updates from multiple clients
        DataStream<FederatedUpdate> federatedUpdates = generateFederatedUpdates(env)
            .assignTimestampsAndWatermarks(watermarkStrategy);
        
        // Demo 1: Secure Model Aggregation
        runSecureAggregation(federatedUpdates);
        
        // Demo 2: Adaptive Federated Optimization
        runAdaptiveFederation(federatedUpdates);
        
        // Demo 3: Privacy-Preserving Learning
        runPrivacyPreservingLearning(federatedUpdates);
        
        // Demo 4: Byzantine Fault Tolerance
        runByzantineTolerance(federatedUpdates);
        
        // Demo 5: Cross-Silo Federation Coordination
        runCrossSiloFederation(federatedUpdates);
        
        // Demo 6: Model Performance Monitoring
        runModelPerformanceMonitoring(federatedUpdates);
        
        LOG.info("✅ Federated Learning demo configured - executing...");
        env.execute("Advanced Federated Learning Job");
    }
    
    /**
     * Generate federated learning updates from multiple clients
     */
    private static DataStream<FederatedUpdate> generateFederatedUpdates(StreamExecutionEnvironment env) {
        return env.addSource(new FederatedUpdateGenerator())
                  .name("Federated Update Generator");
    }
    
    /**
     * Secure model aggregation using cryptographic protocols
     */
    private static void runSecureAggregation(DataStream<FederatedUpdate> updates) {
        LOG.info("🔐 Running Secure Model Aggregation");
        
        updates.keyBy(FederatedUpdate::getModelId)
               .window(TumblingEventTimeWindows.of(Time.minutes(5)))
               .process(new SecureAggregationProcessor())
               .name("Secure Aggregation")
               .print("Aggregated Models");
    }
    
    /**
     * Adaptive federated optimization
     */
    private static void runAdaptiveFederation(DataStream<FederatedUpdate> updates) {
        LOG.info("🎯 Running Adaptive Federated Optimization");
        
        updates.keyBy(FederatedUpdate::getModelId)
               .process(new AdaptiveFederationProcessor())
               .name("Adaptive Federation")
               .print("Adaptive Learning Decisions");
    }
    
    /**
     * Privacy-preserving learning with differential privacy
     */
    private static void runPrivacyPreservingLearning(DataStream<FederatedUpdate> updates) {
        LOG.info("🛡️ Running Privacy-Preserving Learning");
        
        updates.map(new DifferentialPrivacyFunction())
               .keyBy(update -> update.modelId)
               .window(TumblingEventTimeWindows.of(Time.minutes(3)))
               .process(new PrivacyAnalysisProcessor())
               .name("Privacy Analysis")
               .print("Privacy Metrics");
    }
    
    /**
     * Byzantine fault tolerance for robust aggregation
     */
    private static void runByzantineTolerance(DataStream<FederatedUpdate> updates) {
        LOG.info("⚔️ Running Byzantine Fault Tolerance");
        
        updates.keyBy(FederatedUpdate::getModelId)
               .window(TumblingEventTimeWindows.of(Time.minutes(4)))
               .process(new ByzantineToleranceProcessor())
               .name("Byzantine Tolerance")
               .print("Robust Aggregation Results");
    }
    
    /**
     * Cross-silo federation coordination
     */
    private static void runCrossSiloFederation(DataStream<FederatedUpdate> updates) {
        LOG.info("🏢 Running Cross-Silo Federation");
        
        updates.filter(update -> update.clientType == ClientType.ORGANIZATION)
               .keyBy(FederatedUpdate::getModelId)
               .process(new CrossSiloCoordinationProcessor())
               .name("Cross-Silo Coordination")
               .print("Silo Coordination Events");
    }
    
    /**
     * Model performance monitoring across federation
     */
    private static void runModelPerformanceMonitoring(DataStream<FederatedUpdate> updates) {
        LOG.info("📊 Running Model Performance Monitoring");
        
        updates.keyBy(FederatedUpdate::getModelId)
               .window(TumblingEventTimeWindows.of(Time.minutes(2)))
               .process(new PerformanceMonitoringProcessor())
               .name("Performance Monitoring")
               .print("Performance Metrics");
    }
    
    /**
     * Federated update data model
     */
    public static class FederatedUpdate {
        public String clientId;
        public String modelId;
        public int round;
        public double[] modelWeights;
        public double[] gradients;
        public Map<String, Double> metrics;
        public long timestamp;
        public ClientType clientType;
        public String dataDistribution;
        public int localEpochs;
        public double learningRate;
        public boolean isMalicious; // For Byzantine testing
        
        public FederatedUpdate() {
            this.metrics = new HashMap<>();
        }
        
        public FederatedUpdate(String clientId, String modelId, int round, 
                             double[] modelWeights, double[] gradients, long timestamp) {
            this.clientId = clientId;
            this.modelId = modelId;
            this.round = round;
            this.modelWeights = modelWeights;
            this.gradients = gradients;
            this.timestamp = timestamp;
            this.metrics = new HashMap<>();
        }
        
        public String getClientId() { return clientId; }
        public String getModelId() { return modelId; }
        public int getRound() { return round; }
        public long getTimestamp() { return timestamp; }
        
        @Override
        public String toString() {
            return String.format("FederatedUpdate{client='%s', model='%s', round=%d, type=%s, malicious=%s}", 
                               clientId, modelId, round, clientType, isMalicious);
        }
    }
    
    /**
     * Client types in federated learning
     */
    public enum ClientType {
        MOBILE_DEVICE, EDGE_DEVICE, ORGANIZATION, IOT_DEVICE, CLOUD_NODE
    }
    
    /**
     * Secure aggregation processor
     */
    public static class SecureAggregationProcessor extends ProcessWindowFunction<FederatedUpdate, String, String, TimeWindow> {
        
        @Override
        public void process(String modelId, Context context, 
                          Iterable<FederatedUpdate> updates, 
                          Collector<String> out) throws Exception {
            
            List<FederatedUpdate> updateList = new ArrayList<>();
            for (FederatedUpdate update : updates) {
                updateList.add(update);
            }
            
            if (updateList.isEmpty()) return;
            
            // Perform secure aggregation
            SecureAggregationResult result = performSecureAggregation(updateList);
            
            out.collect(String.format(
                "🔐 Secure Aggregation [%s]: participants=%d, rounds=%s, convergence=%.4f, security_level=%.2f", 
                modelId, result.participantCount, result.rounds, result.convergenceMetric, result.securityLevel
            ));
        }
        
        private SecureAggregationResult performSecureAggregation(List<FederatedUpdate> updates) {
            int participants = updates.size();
            
            // Simulate cryptographic secure aggregation
            double[] aggregatedWeights = null;
            Set<Integer> rounds = new HashSet<>();
            
            for (FederatedUpdate update : updates) {
                rounds.add(update.round);
                
                if (aggregatedWeights == null) {
                    aggregatedWeights = new double[update.modelWeights.length];
                }
                
                // Add encrypted weights (simulated)
                for (int i = 0; i < update.modelWeights.length; i++) {
                    aggregatedWeights[i] += update.modelWeights[i];
                }
            }
            
            // Average the weights
            if (aggregatedWeights != null) {
                for (int i = 0; i < aggregatedWeights.length; i++) {
                    aggregatedWeights[i] /= participants;
                }
            }
            
            // Calculate convergence metric (simplified)
            double convergence = calculateConvergence(updates);
            
            // Calculate security level based on protocol
            double securityLevel = calculateSecurityLevel(participants);
            
            return new SecureAggregationResult(participants, rounds, convergence, securityLevel, aggregatedWeights);
        }
        
        private double calculateConvergence(List<FederatedUpdate> updates) {
            if (updates.size() < 2) return 1.0;
            
            // Calculate variance in model weights as convergence indicator
            double[] meanWeights = new double[updates.get(0).modelWeights.length];
            
            // Calculate mean
            for (FederatedUpdate update : updates) {
                for (int i = 0; i < meanWeights.length; i++) {
                    meanWeights[i] += update.modelWeights[i];
                }
            }
            for (int i = 0; i < meanWeights.length; i++) {
                meanWeights[i] /= updates.size();
            }
            
            // Calculate variance
            double totalVariance = 0;
            for (FederatedUpdate update : updates) {
                for (int i = 0; i < meanWeights.length; i++) {
                    totalVariance += Math.pow(update.modelWeights[i] - meanWeights[i], 2);
                }
            }
            
            return 1.0 / (1.0 + totalVariance / (updates.size() * meanWeights.length));
        }
        
        private double calculateSecurityLevel(int participants) {
            // Security level increases with more participants (simplified)
            return Math.min(1.0, participants / 20.0);
        }
    }
    
    /**
     * Adaptive federation processor
     */
    public static class AdaptiveFederationProcessor extends KeyedProcessFunction<String, FederatedUpdate, String> {
        
        private ValueState<FederationState> federationState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            federationState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("federationState", FederationState.class));
        }
        
        @Override
        public void processElement(FederatedUpdate update, Context ctx, Collector<String> out) throws Exception {
            FederationState state = federationState.value();
            if (state == null) {
                state = new FederationState();
            }
            
            // Update federation state
            state.addUpdate(update);
            
            // Make adaptive decisions
            AdaptiveDecision decision = makeAdaptiveDecision(state, update);
            
            if (decision.shouldAdjust) {
                out.collect(String.format(
                    "🎯 Adaptive Decision [%s]: action=%s, reason=%s, newLR=%.4f, newEpochs=%d", 
                    update.modelId, decision.action, decision.reason, decision.newLearningRate, decision.newLocalEpochs
                ));
            }
            
            federationState.update(state);
        }
        
        private AdaptiveDecision makeAdaptiveDecision(FederationState state, FederatedUpdate update) {
            AdaptiveDecision decision = new AdaptiveDecision();
            
            // Analyze learning progress
            double progressRate = state.calculateProgressRate();
            double clientDiversity = state.calculateClientDiversity();
            double convergenceRate = state.calculateConvergenceRate();
            
            // Adaptive learning rate adjustment
            if (convergenceRate < 0.1 && progressRate < 0.05) {
                decision.shouldAdjust = true;
                decision.action = "INCREASE_LEARNING_RATE";
                decision.reason = "Slow convergence detected";
                decision.newLearningRate = Math.min(0.1, update.learningRate * 1.5);
            } else if (convergenceRate > 0.8) {
                decision.shouldAdjust = true;
                decision.action = "DECREASE_LEARNING_RATE";
                decision.reason = "Fast convergence, reducing overfitting risk";
                decision.newLearningRate = Math.max(0.001, update.learningRate * 0.8);
            }
            
            // Adaptive local epochs adjustment
            if (clientDiversity < 0.3) {
                decision.shouldAdjust = true;
                decision.action = "INCREASE_LOCAL_EPOCHS";
                decision.reason = "Low client diversity, increase local training";
                decision.newLocalEpochs = Math.min(10, update.localEpochs + 2);
            } else if (clientDiversity > 0.8) {
                decision.shouldAdjust = true;
                decision.action = "DECREASE_LOCAL_EPOCHS";
                decision.reason = "High client diversity, reduce local training";
                decision.newLocalEpochs = Math.max(1, update.localEpochs - 1);
            }
            
            return decision;
        }
    }
    
    /**
     * Differential privacy function
     */
    public static class DifferentialPrivacyFunction extends RichMapFunction<FederatedUpdate, FederatedUpdate> {
        private final double epsilon = 1.0; // Privacy budget
        private final double delta = 1e-5; // Privacy parameter
        private final Random random = new Random();
        
        @Override
        public FederatedUpdate map(FederatedUpdate update) throws Exception {
            // Apply differential privacy to gradients
            double[] noisyGradients = addDifferentialPrivacyNoise(update.gradients);
            
            // Create privacy-preserved update
            FederatedUpdate privateUpdate = new FederatedUpdate(
                update.clientId, update.modelId, update.round, 
                update.modelWeights, noisyGradients, update.timestamp
            );
            
            // Copy other fields
            privateUpdate.clientType = update.clientType;
            privateUpdate.dataDistribution = update.dataDistribution;
            privateUpdate.localEpochs = update.localEpochs;
            privateUpdate.learningRate = update.learningRate;
            privateUpdate.isMalicious = update.isMalicious;
            
            // Add privacy metrics
            privateUpdate.metrics.put("privacy_epsilon", epsilon);
            privateUpdate.metrics.put("privacy_delta", delta);
            privateUpdate.metrics.put("noise_scale", calculateNoiseScale());
            
            return privateUpdate;
        }
        
        private double[] addDifferentialPrivacyNoise(double[] gradients) {
            double[] noisyGradients = new double[gradients.length];
            double noiseScale = calculateNoiseScale();
            
            for (int i = 0; i < gradients.length; i++) {
                // Add Gaussian noise for differential privacy
                double noise = random.nextGaussian() * noiseScale;
                noisyGradients[i] = gradients[i] + noise;
            }
            
            return noisyGradients;
        }
        
        private double calculateNoiseScale() {
            // Calculate noise scale based on sensitivity and privacy parameters
            double sensitivity = 1.0; // Gradient sensitivity (simplified)
            return (2.0 * sensitivity * Math.log(1.25 / delta)) / epsilon;
        }
    }
    
    /**
     * Privacy analysis processor
     */
    public static class PrivacyAnalysisProcessor extends ProcessWindowFunction<FederatedUpdate, String, String, TimeWindow> {
        
        @Override
        public void process(String key, Context context, 
                          Iterable<FederatedUpdate> updates, 
                          Collector<String> out) throws Exception {
            
            PrivacyMetrics metrics = calculatePrivacyMetrics(updates);
            
            out.collect(String.format(
                "🛡️ Privacy Analysis [%s]: epsilon_used=%.3f, delta_used=%.2e, privacy_loss=%.3f, utility_score=%.3f", 
                key, metrics.epsilonUsed, metrics.deltaUsed, metrics.privacyLoss, metrics.utilityScore
            ));
        }
        
        private PrivacyMetrics calculatePrivacyMetrics(Iterable<FederatedUpdate> updates) {
            double totalEpsilon = 0;
            double totalDelta = 0;
            int count = 0;
            double totalUtility = 0;
            
            for (FederatedUpdate update : updates) {
                if (update.metrics.containsKey("privacy_epsilon")) {
                    totalEpsilon += update.metrics.get("privacy_epsilon");
                    totalDelta += update.metrics.get("privacy_delta");
                    count++;
                    
                    // Calculate utility loss due to noise
                    double noiseScale = update.metrics.getOrDefault("noise_scale", 0.0);
                    double utility = 1.0 / (1.0 + noiseScale); // Simplified utility metric
                    totalUtility += utility;
                }
            }
            
            double privacyLoss = totalEpsilon; // Simplified privacy loss
            double avgUtility = count > 0 ? totalUtility / count : 0;
            
            return new PrivacyMetrics(totalEpsilon, totalDelta, privacyLoss, avgUtility);
        }
    }
    
    /**
     * Byzantine tolerance processor
     */
    public static class ByzantineToleranceProcessor extends ProcessWindowFunction<FederatedUpdate, String, String, TimeWindow> {
        
        @Override
        public void process(String key, Context context, 
                          Iterable<FederatedUpdate> updates, 
                          Collector<String> out) throws Exception {
            
            List<FederatedUpdate> updateList = new ArrayList<>();
            for (FederatedUpdate update : updates) {
                updateList.add(update);
            }
            
            if (updateList.isEmpty()) return;
            
            // Detect and handle Byzantine clients
            ByzantineAnalysis analysis = detectByzantineClients(updateList);
            
            // Perform robust aggregation
            RobustAggregationResult result = performRobustAggregation(updateList, analysis.byzantineClients);
            
            out.collect(String.format(
                "⚔️ Byzantine Tolerance [%s]: total_clients=%d, byzantine_detected=%d, filtered=%d, robustness=%.3f", 
                key, analysis.totalClients, analysis.byzantineClients.size(), 
                result.filteredCount, result.robustnessScore
            ));
        }
        
        private ByzantineAnalysis detectByzantineClients(List<FederatedUpdate> updates) {
            Set<String> byzantineClients = new HashSet<>();
            
            // Detection method 1: Check for explicitly marked malicious clients
            for (FederatedUpdate update : updates) {
                if (update.isMalicious) {
                    byzantineClients.add(update.clientId);
                }
            }
            
            // Detection method 2: Statistical outlier detection
            byzantineClients.addAll(detectStatisticalOutliers(updates));
            
            return new ByzantineAnalysis(updates.size(), byzantineClients);
        }
        
        private Set<String> detectStatisticalOutliers(List<FederatedUpdate> updates) {
            Set<String> outliers = new HashSet<>();
            
            if (updates.size() < 3) return outliers;
            
            // Calculate statistics for each weight dimension
            int weightDim = updates.get(0).modelWeights.length;
            
            for (int dim = 0; dim < weightDim; dim++) {
                List<Double> weightValues = new ArrayList<>();
                Map<String, Double> clientWeights = new HashMap<>();
                
                for (FederatedUpdate update : updates) {
                    double weight = update.modelWeights[dim];
                    weightValues.add(weight);
                    clientWeights.put(update.clientId, weight);
                }
                
                // Calculate median and MAD (Median Absolute Deviation)
                Collections.sort(weightValues);
                double median = weightValues.get(weightValues.size() / 2);
                
                List<Double> deviations = new ArrayList<>();
                for (double value : weightValues) {
                    deviations.add(Math.abs(value - median));
                }
                Collections.sort(deviations);
                double mad = deviations.get(deviations.size() / 2);
                
                // Identify outliers using modified z-score
                double threshold = 3.5; // Common threshold
                for (Map.Entry<String, Double> entry : clientWeights.entrySet()) {
                    if (mad > 0) {
                        double modifiedZScore = 0.6745 * (entry.getValue() - median) / mad;
                        if (Math.abs(modifiedZScore) > threshold) {
                            outliers.add(entry.getKey());
                        }
                    }
                }
            }
            
            return outliers;
        }
        
        private RobustAggregationResult performRobustAggregation(List<FederatedUpdate> updates, 
                                                               Set<String> byzantineClients) {
            // Filter out Byzantine clients
            List<FederatedUpdate> filteredUpdates = new ArrayList<>();
            for (FederatedUpdate update : updates) {
                if (!byzantineClients.contains(update.clientId)) {
                    filteredUpdates.add(update);
                }
            }
            
            int filteredCount = updates.size() - filteredUpdates.size();
            
            // Calculate robustness score
            double robustnessScore = filteredUpdates.size() / (double) updates.size();
            
            return new RobustAggregationResult(filteredCount, robustnessScore);
        }
    }
    
    /**
     * Cross-silo coordination processor
     */
    public static class CrossSiloCoordinationProcessor extends KeyedProcessFunction<String, FederatedUpdate, String> {
        
        private MapState<String, SiloInfo> siloState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            siloState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("siloInfo", String.class, SiloInfo.class));
        }
        
        @Override
        public void processElement(FederatedUpdate update, Context ctx, Collector<String> out) throws Exception {
            // Extract silo information from client ID
            String siloId = extractSiloId(update.clientId);
            
            SiloInfo info = siloState.get(siloId);
            if (info == null) {
                info = new SiloInfo(siloId);
            }
            
            info.updateWithClient(update);
            siloState.put(siloId, info);
            
            // Check for coordination events
            if (shouldTriggerCoordination(info)) {
                out.collect(String.format(
                    "🏢 Silo Coordination [%s]: silo=%s, clients=%d, data_samples=%d, avg_accuracy=%.3f", 
                    update.modelId, siloId, info.clientCount, info.totalDataSamples, info.averageAccuracy
                ));
            }
        }
        
        private String extractSiloId(String clientId) {
            // Extract silo identifier from client ID (e.g., "hospital_A_client_001" -> "hospital_A")
            String[] parts = clientId.split("_");
            if (parts.length >= 2) {
                return parts[0] + "_" + parts[1];
            }
            return "unknown_silo";
        }
        
        private boolean shouldTriggerCoordination(SiloInfo info) {
            // Trigger coordination when enough clients from silo have reported
            return info.clientCount >= 5 && (info.clientCount % 5 == 0);
        }
    }
    
    /**
     * Performance monitoring processor
     */
    public static class PerformanceMonitoringProcessor extends ProcessWindowFunction<FederatedUpdate, String, String, TimeWindow> {
        
        @Override
        public void process(String key, Context context, 
                          Iterable<FederatedUpdate> updates, 
                          Collector<String> out) throws Exception {
            
            PerformanceMetrics metrics = calculatePerformanceMetrics(updates);
            
            out.collect(String.format(
                "📊 Performance Metrics [%s]: clients=%d, avg_accuracy=%.3f, communication_cost=%.1f, convergence_rate=%.3f, fairness=%.3f", 
                key, metrics.clientCount, metrics.averageAccuracy, 
                metrics.communicationCost, metrics.convergenceRate, metrics.fairnessScore
            ));
        }
        
        private PerformanceMetrics calculatePerformanceMetrics(Iterable<FederatedUpdate> updates) {
            int clientCount = 0;
            double totalAccuracy = 0;
            double totalCommunicationCost = 0;
            List<Double> accuracies = new ArrayList<>();
            Set<Integer> rounds = new HashSet<>();
            
            for (FederatedUpdate update : updates) {
                clientCount++;
                
                // Get accuracy from metrics
                double accuracy = update.metrics.getOrDefault("accuracy", 0.5 + Math.random() * 0.5);
                totalAccuracy += accuracy;
                accuracies.add(accuracy);
                
                // Calculate communication cost (model size + metadata)
                double commCost = update.modelWeights.length * 4.0 + 1024; // bytes
                totalCommunicationCost += commCost;
                
                rounds.add(update.round);
            }
            
            double averageAccuracy = clientCount > 0 ? totalAccuracy / clientCount : 0;
            double avgCommunicationCost = clientCount > 0 ? totalCommunicationCost / clientCount : 0;
            
            // Calculate convergence rate (based on rounds diversity)
            double convergenceRate = rounds.size() > 1 ? 1.0 / rounds.size() : 1.0;
            
            // Calculate fairness score (based on accuracy variance)
            double fairnessScore = calculateFairnessScore(accuracies);
            
            return new PerformanceMetrics(clientCount, averageAccuracy, avgCommunicationCost, 
                                        convergenceRate, fairnessScore);
        }
        
        private double calculateFairnessScore(List<Double> accuracies) {
            if (accuracies.size() < 2) return 1.0;
            
            double mean = accuracies.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            double variance = accuracies.stream()
                                      .mapToDouble(acc -> Math.pow(acc - mean, 2))
                                      .average().orElse(0);
            
            // Fairness is inversely related to variance
            return 1.0 / (1.0 + variance);
        }
    }
    
    // Helper classes and data structures
    public static class SecureAggregationResult {
        public final int participantCount;
        public final Set<Integer> rounds;
        public final double convergenceMetric;
        public final double securityLevel;
        public final double[] aggregatedWeights;
        
        public SecureAggregationResult(int participantCount, Set<Integer> rounds, 
                                     double convergenceMetric, double securityLevel, 
                                     double[] aggregatedWeights) {
            this.participantCount = participantCount;
            this.rounds = rounds;
            this.convergenceMetric = convergenceMetric;
            this.securityLevel = securityLevel;
            this.aggregatedWeights = aggregatedWeights;
        }
    }
    
    public static class FederationState {
        private final List<FederatedUpdate> recentUpdates = new ArrayList<>();
        private final Map<String, Double> clientPerformance = new HashMap<>();
        
        public void addUpdate(FederatedUpdate update) {
            recentUpdates.add(update);
            
            // Keep only recent updates
            if (recentUpdates.size() > 100) {
                recentUpdates.remove(0);
            }
            
            // Track client performance
            double accuracy = update.metrics.getOrDefault("accuracy", 0.5);
            clientPerformance.put(update.clientId, accuracy);
        }
        
        public double calculateProgressRate() {
            if (recentUpdates.size() < 10) return 0.5;
            
            // Compare recent vs older performance
            int half = recentUpdates.size() / 2;
            double recentAvg = recentUpdates.subList(half, recentUpdates.size())
                                          .stream()
                                          .mapToDouble(u -> u.metrics.getOrDefault("accuracy", 0.5))
                                          .average().orElse(0.5);
            
            double olderAvg = recentUpdates.subList(0, half)
                                         .stream()
                                         .mapToDouble(u -> u.metrics.getOrDefault("accuracy", 0.5))
                                         .average().orElse(0.5);
            
            return recentAvg - olderAvg;
        }
        
        public double calculateClientDiversity() {
            if (clientPerformance.isEmpty()) return 0.5;
            
            double[] performances = clientPerformance.values().stream()
                                                   .mapToDouble(Double::doubleValue)
                                                   .toArray();
            
            double mean = Arrays.stream(performances).average().orElse(0);
            double variance = Arrays.stream(performances)
                                   .map(p -> Math.pow(p - mean, 2))
                                   .average().orElse(0);
            
            return Math.sqrt(variance); // Standard deviation as diversity measure
        }
        
        public double calculateConvergenceRate() {
            if (recentUpdates.size() < 5) return 0.5;
            
            // Calculate convergence based on weight stability
            double totalVariation = 0;
            int comparisons = 0;
            
            for (int i = 1; i < recentUpdates.size(); i++) {
                FederatedUpdate current = recentUpdates.get(i);
                FederatedUpdate previous = recentUpdates.get(i - 1);
                
                if (current.modelWeights.length == previous.modelWeights.length) {
                    double variation = 0;
                    for (int j = 0; j < current.modelWeights.length; j++) {
                        variation += Math.abs(current.modelWeights[j] - previous.modelWeights[j]);
                    }
                    totalVariation += variation;
                    comparisons++;
                }
            }
            
            double avgVariation = comparisons > 0 ? totalVariation / comparisons : 1.0;
            return 1.0 / (1.0 + avgVariation); // Higher variation = lower convergence
        }
    }
    
    public static class AdaptiveDecision {
        public boolean shouldAdjust = false;
        public String action = "";
        public String reason = "";
        public double newLearningRate = 0.01;
        public int newLocalEpochs = 1;
    }
    
    public static class PrivacyMetrics {
        public final double epsilonUsed;
        public final double deltaUsed;
        public final double privacyLoss;
        public final double utilityScore;
        
        public PrivacyMetrics(double epsilonUsed, double deltaUsed, double privacyLoss, double utilityScore) {
            this.epsilonUsed = epsilonUsed;
            this.deltaUsed = deltaUsed;
            this.privacyLoss = privacyLoss;
            this.utilityScore = utilityScore;
        }
    }
    
    public static class ByzantineAnalysis {
        public final int totalClients;
        public final Set<String> byzantineClients;
        
        public ByzantineAnalysis(int totalClients, Set<String> byzantineClients) {
            this.totalClients = totalClients;
            this.byzantineClients = byzantineClients;
        }
    }
    
    public static class RobustAggregationResult {
        public final int filteredCount;
        public final double robustnessScore;
        
        public RobustAggregationResult(int filteredCount, double robustnessScore) {
            this.filteredCount = filteredCount;
            this.robustnessScore = robustnessScore;
        }
    }
    
    public static class SiloInfo {
        public final String siloId;
        public int clientCount = 0;
        public long totalDataSamples = 0;
        public double averageAccuracy = 0;
        public long lastUpdateTime = 0;
        
        public SiloInfo(String siloId) {
            this.siloId = siloId;
        }
        
        public void updateWithClient(FederatedUpdate update) {
            clientCount++;
            totalDataSamples += update.metrics.getOrDefault("data_samples", 1000.0).longValue();
            
            double accuracy = update.metrics.getOrDefault("accuracy", 0.5);
            averageAccuracy = (averageAccuracy * (clientCount - 1) + accuracy) / clientCount;
            
            lastUpdateTime = update.timestamp;
        }
    }
    
    public static class PerformanceMetrics {
        public final int clientCount;
        public final double averageAccuracy;
        public final double communicationCost;
        public final double convergenceRate;
        public final double fairnessScore;
        
        public PerformanceMetrics(int clientCount, double averageAccuracy, double communicationCost,
                                double convergenceRate, double fairnessScore) {
            this.clientCount = clientCount;
            this.averageAccuracy = averageAccuracy;
            this.communicationCost = communicationCost;
            this.convergenceRate = convergenceRate;
            this.fairnessScore = fairnessScore;
        }
    }
    
    /**
     * Federated update generator
     */
    public static class FederatedUpdateGenerator extends org.apache.flink.streaming.api.functions.source.SourceFunction<FederatedUpdate> {
        private volatile boolean running = true;
        private final Random random = new Random();
        private final String[] modelIds = {"fraud_detection", "recommendation", "nlp_classification", "image_recognition"};
        private final String[] organizations = {"hospital_A", "bank_B", "tech_C", "retail_D", "telecom_E"};
        private final ClientType[] clientTypes = ClientType.values();
        
        @Override
        public void run(SourceContext<FederatedUpdate> ctx) throws Exception {
            int round = 1;
            
            while (running) {
                // Generate updates from multiple clients
                String modelId = modelIds[random.nextInt(modelIds.length)];
                
                // Generate 5-15 client updates per round
                int clientsInRound = 5 + random.nextInt(11);
                
                for (int i = 0; i < clientsInRound; i++) {
                    FederatedUpdate update = generateClientUpdate(modelId, round);
                    ctx.collect(update);
                    
                    // Small delay between client updates
                    Thread.sleep(random.nextInt(500) + 100);
                }
                
                round++;
                
                // Delay between rounds
                Thread.sleep(random.nextInt(3000) + 2000);
            }
        }
        
        private FederatedUpdate generateClientUpdate(String modelId, int round) {
            // Generate client ID
            String org = organizations[random.nextInt(organizations.length)];
            String clientId = org + "_client_" + String.format("%03d", random.nextInt(100));
            
            // Generate model weights and gradients
            int modelSize = 10 + random.nextInt(20); // 10-30 parameters
            double[] weights = new double[modelSize];
            double[] gradients = new double[modelSize];
            
            for (int i = 0; i < modelSize; i++) {
                weights[i] = random.nextGaussian() * 0.1; // Small random weights
                gradients[i] = random.nextGaussian() * 0.01; // Small gradients
            }
            
            FederatedUpdate update = new FederatedUpdate(
                clientId, modelId, round, weights, gradients, System.currentTimeMillis()
            );
            
            // Set additional properties
            update.clientType = clientTypes[random.nextInt(clientTypes.length)];
            update.localEpochs = 1 + random.nextInt(5);
            update.learningRate = 0.001 + random.nextDouble() * 0.099;
            update.isMalicious = random.nextDouble() < 0.05; // 5% malicious
            
            // Add realistic metrics
            update.metrics.put("accuracy", 0.6 + random.nextDouble() * 0.35); // 60-95% accuracy
            update.metrics.put("loss", random.nextDouble() * 2.0); // 0-2.0 loss
            update.metrics.put("data_samples", (double) (100 + random.nextInt(9900))); // 100-10000 samples
            update.metrics.put("training_time", 10.0 + random.nextDouble() * 290.0); // 10-300 seconds
            
            // Data distribution simulation
            String[] distributions = {"iid", "non_iid_label", "non_iid_feature", "clustered"};
            update.dataDistribution = distributions[random.nextInt(distributions.length)];
            
            return update;
        }
        
        @Override
        public void cancel() {
            running = false;
        }
    }
}
