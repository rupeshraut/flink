package com.example.flink.edge;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Edge AI Demo for Apache Flink
 * 
 * This demo showcases distributed edge computing with AI capabilities:
 * - Federated model inference at edge nodes
 * - Adaptive model compression and quantization
 * - Real-time model optimization for resource constraints
 * - Edge-cloud hybrid processing
 * - Latency-aware computation offloading
 * - Collaborative edge intelligence
 */
public class EdgeAIJob {
    
    private static final Logger logger = LoggerFactory.getLogger(EdgeAIJob.class);
    
    /**
     * Represents an edge device with AI capabilities
     */
    public static class EdgeDevice {
        public final String deviceId;
        public final String location;
        public final double cpuCapacity;
        public final double memoryCapacity;
        public final double batteryLevel;
        public final double networkLatency;
        public final Set<String> availableModels;
        public final long timestamp;
        
        public EdgeDevice(String deviceId, String location, double cpuCapacity, double memoryCapacity,
                         double batteryLevel, double networkLatency, Set<String> availableModels, long timestamp) {
            this.deviceId = deviceId;
            this.location = location;
            this.cpuCapacity = cpuCapacity;
            this.memoryCapacity = memoryCapacity;
            this.batteryLevel = batteryLevel;
            this.networkLatency = networkLatency;
            this.availableModels = availableModels;
            this.timestamp = timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("EdgeDevice{id=%s, location=%s, cpu=%.1f%%, mem=%.1f%%, battery=%.1f%%, latency=%.1fms}", 
                               deviceId, location, cpuCapacity * 100, memoryCapacity * 100, batteryLevel * 100, networkLatency);
        }
    }
    
    /**
     * AI inference request for edge processing
     */
    public static class InferenceRequest {
        public final String requestId;
        public final String modelType;
        public final double[] inputData;
        public final String priority;
        public final double maxLatency;
        public final String sourceDevice;
        public final long timestamp;
        
        public InferenceRequest(String requestId, String modelType, double[] inputData, 
                              String priority, double maxLatency, String sourceDevice, long timestamp) {
            this.requestId = requestId;
            this.modelType = modelType;
            this.inputData = inputData;
            this.priority = priority;
            this.maxLatency = maxLatency;
            this.sourceDevice = sourceDevice;
            this.timestamp = timestamp;
        }
    }
    
    /**
     * Edge inference result with performance metrics
     */
    public static class EdgeInferenceResult {
        public final String requestId;
        public final String assignedDevice;
        public final String prediction;
        public final double confidence;
        public final double processingTime;
        public final double energyConsumed;
        public final boolean wasOffloaded;
        public final Map<String, Object> metrics;
        public final long timestamp;
        
        public EdgeInferenceResult(String requestId, String assignedDevice, String prediction, 
                                 double confidence, double processingTime, double energyConsumed,
                                 boolean wasOffloaded, Map<String, Object> metrics, long timestamp) {
            this.requestId = requestId;
            this.assignedDevice = assignedDevice;
            this.prediction = prediction;
            this.confidence = confidence;
            this.processingTime = processingTime;
            this.energyConsumed = energyConsumed;
            this.wasOffloaded = wasOffloaded;
            this.metrics = metrics;
            this.timestamp = timestamp;
        }
    }
    
    /**
     * Model optimization configuration for edge deployment
     */
    public static class ModelOptimization {
        public final String modelId;
        public final String optimizationType;
        public final double compressionRatio;
        public final int quantizationBits;
        public final double accuracyLoss;
        public final double speedupFactor;
        public final double memoryReduction;
        
        public ModelOptimization(String modelId, String optimizationType, double compressionRatio,
                               int quantizationBits, double accuracyLoss, double speedupFactor, double memoryReduction) {
            this.modelId = modelId;
            this.optimizationType = optimizationType;
            this.compressionRatio = compressionRatio;
            this.quantizationBits = quantizationBits;
            this.accuracyLoss = accuracyLoss;
            this.speedupFactor = speedupFactor;
            this.memoryReduction = memoryReduction;
        }
    }
    
    /**
     * Generates edge device status updates
     */
    public static class EdgeDeviceStatusGenerator implements MapFunction<Long, EdgeDevice> {
        private final String[] locations = {"factory_floor", "retail_store", "smart_home", "autonomous_vehicle", "drone"};
        private final String[] models = {"object_detection", "sentiment_analysis", "anomaly_detection", "predictive_maintenance", "speech_recognition"};
        
        @Override
        public EdgeDevice map(Long value) throws Exception {
            String deviceId = "edge_device_" + (value % 20);
            String location = locations[ThreadLocalRandom.current().nextInt(locations.length)];
            
            // Simulate varying resource availability
            double cpuCapacity = 0.3 + ThreadLocalRandom.current().nextDouble(0.7);
            double memoryCapacity = 0.4 + ThreadLocalRandom.current().nextDouble(0.6);
            double batteryLevel = 0.2 + ThreadLocalRandom.current().nextDouble(0.8);
            double networkLatency = 10 + ThreadLocalRandom.current().nextDouble(190);
            
            // Randomly assign available models
            Set<String> availableModels = new HashSet<>();
            int numModels = 1 + ThreadLocalRandom.current().nextInt(3);
            for (int i = 0; i < numModels; i++) {
                availableModels.add(models[ThreadLocalRandom.current().nextInt(models.length)]);
            }
            
            return new EdgeDevice(deviceId, location, cpuCapacity, memoryCapacity, 
                                batteryLevel, networkLatency, availableModels, System.currentTimeMillis());
        }
    }
    
    /**
     * Generates AI inference requests
     */
    public static class InferenceRequestGenerator implements MapFunction<Long, InferenceRequest> {
        private final String[] modelTypes = {"object_detection", "sentiment_analysis", "anomaly_detection", "predictive_maintenance", "speech_recognition"};
        private final String[] priorities = {"low", "medium", "high", "critical"};
        
        @Override
        public InferenceRequest map(Long value) throws Exception {
            String requestId = "req_" + value;
            String modelType = modelTypes[ThreadLocalRandom.current().nextInt(modelTypes.length)];
            
            // Generate input data
            double[] inputData = new double[10];
            for (int i = 0; i < inputData.length; i++) {
                inputData[i] = ThreadLocalRandom.current().nextGaussian();
            }
            
            String priority = priorities[ThreadLocalRandom.current().nextInt(priorities.length)];
            double maxLatency = 50 + ThreadLocalRandom.current().nextDouble(450);
            String sourceDevice = "source_" + (value % 10);
            
            return new InferenceRequest(requestId, modelType, inputData, priority, 
                                      maxLatency, sourceDevice, System.currentTimeMillis());
        }
    }
    
    /**
     * Implements intelligent workload placement for edge devices
     */
    public static class EdgeWorkloadPlacer extends RichMapFunction<Tuple2<InferenceRequest, EdgeDevice>, EdgeInferenceResult> {
        private transient MapState<String, EdgeDevice> availableDevicesState;
        private transient ValueState<Map<String, Double>> deviceLoadState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            availableDevicesState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("availableDevices", Types.STRING, Types.POJO(EdgeDevice.class))
            );
            deviceLoadState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("deviceLoad", Types.MAP(Types.STRING, Types.DOUBLE))
            );
        }
        
        @Override
        public EdgeInferenceResult map(Tuple2<InferenceRequest, EdgeDevice> input) throws Exception {
            InferenceRequest request = input.f0;
            EdgeDevice device = input.f1;
            
            // Update device availability
            availableDevicesState.put(device.deviceId, device);
            
            // Update device load tracking
            Map<String, Double> deviceLoads = deviceLoadState.value();
            if (deviceLoads == null) {
                deviceLoads = new HashMap<>();
            }
            
            // Find optimal device for request
            String assignedDevice = findOptimalDevice(request, deviceLoads);
            EdgeDevice selectedDevice = device; // Simplified for demo
            
            // Simulate inference execution
            boolean wasOffloaded = shouldOffload(request, selectedDevice);
            double processingTime = calculateProcessingTime(request, selectedDevice, wasOffloaded);
            double energyConsumed = calculateEnergyConsumption(request, selectedDevice, processingTime);
            
            // Update device load
            double currentLoad = deviceLoads.getOrDefault(assignedDevice, 0.0);
            deviceLoads.put(assignedDevice, Math.min(1.0, currentLoad + 0.1));
            deviceLoadState.update(deviceLoads);
            
            // Generate prediction result
            String[] predictions = {"detected", "normal", "anomaly", "positive", "negative"};
            String prediction = predictions[ThreadLocalRandom.current().nextInt(predictions.length)];
            double confidence = 0.6 + ThreadLocalRandom.current().nextDouble(0.4);
            
            // Collect performance metrics
            Map<String, Object> metrics = new HashMap<>();
            metrics.put("device_cpu_usage", selectedDevice.cpuCapacity);
            metrics.put("device_memory_usage", selectedDevice.memoryCapacity);
            metrics.put("network_latency", selectedDevice.networkLatency);
            metrics.put("battery_level", selectedDevice.batteryLevel);
            metrics.put("queue_length", currentLoad);
            
            return new EdgeInferenceResult(request.requestId, assignedDevice, prediction, 
                                         confidence, processingTime, energyConsumed, 
                                         wasOffloaded, metrics, System.currentTimeMillis());
        }
        
        private String findOptimalDevice(InferenceRequest request, Map<String, Double> deviceLoads) throws Exception {
            String bestDevice = "edge_device_0"; // Default
            double bestScore = -1.0;
            
            for (Map.Entry<String, EdgeDevice> entry : availableDevicesState.entries()) {
                EdgeDevice device = entry.getValue();
                
                // Check if device supports the required model
                if (!device.availableModels.contains(request.modelType)) {
                    continue;
                }
                
                // Calculate placement score
                double load = deviceLoads.getOrDefault(device.deviceId, 0.0);
                double score = calculatePlacementScore(request, device, load);
                
                if (score > bestScore) {
                    bestScore = score;
                    bestDevice = device.deviceId;
                }
            }
            
            return bestDevice;
        }
        
        private double calculatePlacementScore(InferenceRequest request, EdgeDevice device, double load) {
            // Multi-criteria scoring: latency, battery, load, capability
            double latencyScore = Math.max(0, 1.0 - (device.networkLatency / 200.0));
            double batteryScore = device.batteryLevel;
            double loadScore = Math.max(0, 1.0 - load);
            double capabilityScore = device.cpuCapacity * device.memoryCapacity;
            
            // Priority-based weighting
            double urgencyWeight = request.priority.equals("critical") ? 2.0 : 1.0;
            
            return urgencyWeight * (0.3 * latencyScore + 0.2 * batteryScore + 0.3 * loadScore + 0.2 * capabilityScore);
        }
        
        private boolean shouldOffload(InferenceRequest request, EdgeDevice device) {
            // Offload to cloud if device resources are too low or latency requirements are relaxed
            return device.cpuCapacity < 0.3 || device.batteryLevel < 0.2 || request.maxLatency > 300;
        }
        
        private double calculateProcessingTime(InferenceRequest request, EdgeDevice device, boolean offloaded) {
            double baseTime = offloaded ? 100 + device.networkLatency : 20;
            double complexityFactor = request.inputData.length / 10.0;
            double resourceFactor = 1.0 / Math.max(0.1, device.cpuCapacity);
            
            return baseTime * complexityFactor * resourceFactor;
        }
        
        private double calculateEnergyConsumption(InferenceRequest request, EdgeDevice device, double processingTime) {
            // Simplified energy model
            double baseConsumption = processingTime * 0.1; // Base processing energy
            double cpuLoad = 1.0 / Math.max(0.1, device.cpuCapacity);
            return baseConsumption * cpuLoad;
        }
    }
    
    /**
     * Implements adaptive model optimization for edge deployment
     */
    public static class AdaptiveModelOptimizer extends ProcessWindowFunction<EdgeDevice, ModelOptimization, String, TimeWindow> {
        
        @Override
        public void process(String location, Context context, Iterable<EdgeDevice> devices, 
                          Collector<ModelOptimization> out) throws Exception {
            
            List<EdgeDevice> deviceList = new ArrayList<>();
            devices.forEach(deviceList::add);
            
            if (deviceList.isEmpty()) return;
            
            // Analyze resource constraints across devices in this location
            double avgCpuCapacity = deviceList.stream().mapToDouble(d -> d.cpuCapacity).average().orElse(0.5);
            double avgMemoryCapacity = deviceList.stream().mapToDouble(d -> d.memoryCapacity).average().orElse(0.5);
            double avgBatteryLevel = deviceList.stream().mapToDouble(d -> d.batteryLevel).average().orElse(0.5);
            
            // Determine optimal model optimization strategy
            String optimizationType;
            double compressionRatio;
            int quantizationBits;
            double accuracyLoss;
            double speedupFactor;
            double memoryReduction;
            
            if (avgCpuCapacity < 0.4 || avgMemoryCapacity < 0.4) {
                // Aggressive optimization for resource-constrained devices
                optimizationType = "aggressive_compression";
                compressionRatio = 0.1;
                quantizationBits = 4;
                accuracyLoss = 0.05;
                speedupFactor = 3.0;
                memoryReduction = 0.8;
            } else if (avgBatteryLevel < 0.3) {
                // Energy-efficient optimization
                optimizationType = "energy_efficient";
                compressionRatio = 0.3;
                quantizationBits = 8;
                accuracyLoss = 0.02;
                speedupFactor = 2.0;
                memoryReduction = 0.5;
            } else {
                // Balanced optimization
                optimizationType = "balanced";
                compressionRatio = 0.5;
                quantizationBits = 16;
                accuracyLoss = 0.01;
                speedupFactor = 1.5;
                memoryReduction = 0.3;
            }
            
            // Generate optimization for dominant model type
            Set<String> allModels = new HashSet<>();
            deviceList.forEach(d -> allModels.addAll(d.availableModels));
            
            for (String modelId : allModels) {
                out.collect(new ModelOptimization(modelId, optimizationType, compressionRatio,
                                                quantizationBits, accuracyLoss, speedupFactor, memoryReduction));
            }
        }
    }
    
    /**
     * Implements collaborative edge intelligence
     */
    public static class CollaborativeEdgeProcessor extends ProcessWindowFunction<EdgeInferenceResult, String, String, TimeWindow> {
        
        @Override
        public void process(String key, Context context, Iterable<EdgeInferenceResult> results, 
                          Collector<String> out) throws Exception {
            
            List<EdgeInferenceResult> resultList = new ArrayList<>();
            results.forEach(resultList::add);
            
            if (resultList.size() < 2) return;
            
            // Implement collaborative learning
            Map<String, List<EdgeInferenceResult>> deviceResults = new HashMap<>();
            for (EdgeInferenceResult result : resultList) {
                deviceResults.computeIfAbsent(result.assignedDevice, k -> new ArrayList<>()).add(result);
            }
            
            // Knowledge sharing analysis
            Map<String, Double> deviceAccuracies = new HashMap<>();
            Map<String, Double> deviceLatencies = new HashMap<>();
            
            for (Map.Entry<String, List<EdgeInferenceResult>> entry : deviceResults.entrySet()) {
                String device = entry.getKey();
                List<EdgeInferenceResult> deviceResultList = entry.getValue();
                
                double avgConfidence = deviceResultList.stream()
                    .mapToDouble(r -> r.confidence)
                    .average().orElse(0.0);
                
                double avgLatency = deviceResultList.stream()
                    .mapToDouble(r -> r.processingTime)
                    .average().orElse(0.0);
                
                deviceAccuracies.put(device, avgConfidence);
                deviceLatencies.put(device, avgLatency);
            }
            
            // Find best performing device for knowledge sharing
            String bestDevice = deviceAccuracies.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("unknown");
            
            double bestAccuracy = deviceAccuracies.get(bestDevice);
            double bestLatency = deviceLatencies.get(bestDevice);
            
            // Generate collaboration insights
            StringBuilder collaboration = new StringBuilder();
            collaboration.append("COLLABORATIVE_LEARNING: ");
            collaboration.append("Best performer: ").append(bestDevice)
                         .append(" (accuracy: ").append(String.format("%.3f", bestAccuracy))
                         .append(", latency: ").append(String.format("%.1fms", bestLatency)).append("); ");
            
            collaboration.append("Knowledge sharing opportunities: ");
            for (Map.Entry<String, Double> entry : deviceAccuracies.entrySet()) {
                if (!entry.getKey().equals(bestDevice) && entry.getValue() < bestAccuracy - 0.1) {
                    collaboration.append(entry.getKey()).append(" ");
                }
            }
            
            out.collect(collaboration.toString());
        }
    }
    
    /**
     * Main demo execution
     */
    public static void runDemo(StreamExecutionEnvironment env) throws Exception {
        logger.info("📱🤖 === Edge AI Demo ===");
        logger.info("Demonstrating distributed edge computing with AI...");
        
        // Generate edge device status stream
        DataStream<EdgeDevice> deviceStream = env
            .fromSequence(1, 1000)
            .map(new EdgeDeviceStatusGenerator())
            .name("Edge Device Status");
        
        // Generate inference request stream
        DataStream<InferenceRequest> requestStream = env
            .fromSequence(1, 1000)
            .map(new InferenceRequestGenerator())
            .name("Inference Requests");
        
        // Combine streams for workload placement
        DataStream<EdgeInferenceResult> inferenceStream = requestStream
            .connect(deviceStream)
            .map(new org.apache.flink.streaming.api.functions.co.CoMapFunction<InferenceRequest, EdgeDevice, Tuple2<InferenceRequest, EdgeDevice>>() {
                @Override
                public Tuple2<InferenceRequest, EdgeDevice> map1(InferenceRequest request) throws Exception {
                    // Pair with dummy device for processing
                    EdgeDevice dummyDevice = new EdgeDevice("dummy", "none", 0.5, 0.5, 0.5, 100, new HashSet<>(Arrays.asList("dummy")), 0L);
                    return Tuple2.of(request, dummyDevice);
                }
                
                @Override
                public Tuple2<InferenceRequest, EdgeDevice> map2(EdgeDevice device) throws Exception {
                    // Pair with dummy request for processing
                    InferenceRequest dummyRequest = new InferenceRequest("dummy", "dummy", new double[]{0.0}, "low", 100, "dummy", 0L);
                    return Tuple2.of(dummyRequest, device);
                }
            })
            .keyBy(pair -> "workload")
            .map(new EdgeWorkloadPlacer())
            .name("Edge Workload Placement");
        
        // Adaptive model optimization
        DataStream<ModelOptimization> optimizationStream = deviceStream
            .keyBy(device -> device.location)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .process(new AdaptiveModelOptimizer())
            .name("Adaptive Model Optimization");
        
        // Collaborative edge intelligence
        DataStream<String> collaborativeStream = inferenceStream
            .keyBy(result -> "collaboration")
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            .process(new CollaborativeEdgeProcessor())
            .name("Collaborative Edge Intelligence");
        
        // Edge-cloud hybrid processing analysis
        DataStream<String> hybridAnalysisStream = inferenceStream
            .map(result -> {
                String processingLocation = result.wasOffloaded ? "CLOUD" : "EDGE";
                double efficiency = result.confidence / Math.max(1.0, result.processingTime / 100.0);
                
                return String.format("HYBRID_PROCESSING: %s on %s | Efficiency: %.3f | Energy: %.2fJ | Latency: %.1fms",
                                   result.requestId, processingLocation, efficiency, result.energyConsumed, result.processingTime);
            })
            .name("Edge-Cloud Hybrid Analysis");
        
        // Output streams with detailed logging
        deviceStream
            .map(device -> String.format("📱 Device: %s", device.toString()))
            .print("Edge Devices").setParallelism(1);
        
        inferenceStream
            .map(result -> String.format("🧠 Inference: %s -> %s on %s (%.1fms, %.2fJ)", 
                                       result.requestId, result.prediction, result.assignedDevice, 
                                       result.processingTime, result.energyConsumed))
            .print("Edge Inference").setParallelism(1);
        
        optimizationStream
            .map(opt -> String.format("⚡ Optimization: %s | %s | Compression: %.1f%% | Quantization: %d-bit | Speedup: %.1fx",
                                    opt.modelId, opt.optimizationType, opt.compressionRatio * 100, 
                                    opt.quantizationBits, opt.speedupFactor))
            .print("Model Optimization").setParallelism(1);
        
        collaborativeStream.print("🤝 Collaborative Learning").setParallelism(1);
        hybridAnalysisStream.print("☁️ Hybrid Processing").setParallelism(1);
        
        logger.info("📱🤖 Edge AI features:");
        logger.info("  ✅ Intelligent workload placement across edge devices");
        logger.info("  ✅ Adaptive model compression and quantization");
        logger.info("  ✅ Real-time resource constraint optimization");
        logger.info("  ✅ Edge-cloud hybrid processing decisions");
        logger.info("  ✅ Collaborative edge intelligence");
        logger.info("  ✅ Energy-aware computation scheduling");
        logger.info("  ✅ Latency-optimized model deployment");
        
        env.execute("Edge AI Demo");
    }
}
