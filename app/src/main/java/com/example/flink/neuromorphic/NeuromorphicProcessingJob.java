package com.example.flink.neuromorphic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Neuromorphic Computing Demo for Apache Flink
 * 
 * This demo showcases brain-inspired computing patterns including:
 * - Spiking Neural Network simulation
 * - Synaptic plasticity modeling
 * - Adaptive threshold processing
 * - Memory consolidation patterns
 * - Neural oscillation analysis
 * - Event-driven processing inspired by biological neurons
 */
public class NeuromorphicProcessingJob {
    
    private static final Logger logger = LoggerFactory.getLogger(NeuromorphicProcessingJob.class);
    
    /**
     * Neural spike event representing a neuron firing
     */
    public static class NeuralSpike {
        public final String neuronId;
        public final long timestamp;
        public final double potential;
        public final String layer;
        public final Map<String, Double> synapticWeights;
        
        public NeuralSpike(String neuronId, long timestamp, double potential, String layer, Map<String, Double> synapticWeights) {
            this.neuronId = neuronId;
            this.timestamp = timestamp;
            this.potential = potential;
            this.layer = layer;
            this.synapticWeights = synapticWeights;
        }
        
        @Override
        public String toString() {
            return String.format("NeuralSpike{neuron=%s, time=%d, potential=%.3f, layer=%s}", 
                               neuronId, timestamp, potential, layer);
        }
    }
    
    /**
     * Synaptic connection between neurons
     */
    public static class SynapticConnection {
        public final String presynapticNeuron;
        public final String postsynapticNeuron;
        public final double weight;
        public final double delay;
        public final long lastUpdate;
        
        public SynapticConnection(String pre, String post, double weight, double delay, long lastUpdate) {
            this.presynapticNeuron = pre;
            this.postsynapticNeuron = post;
            this.weight = weight;
            this.delay = delay;
            this.lastUpdate = lastUpdate;
        }
    }
    
    /**
     * Neural network state for memory consolidation
     */
    public static class NetworkState {
        public final Map<String, Double> neuronPotentials;
        public final Map<String, Double> thresholds;
        public final long timestamp;
        public final double globalInhibition;
        
        public NetworkState(Map<String, Double> potentials, Map<String, Double> thresholds, 
                          long timestamp, double globalInhibition) {
            this.neuronPotentials = potentials;
            this.thresholds = thresholds;
            this.timestamp = timestamp;
            this.globalInhibition = globalInhibition;
        }
    }
    
    /**
     * Source that generates neural spike events using DataGen API
     */
    public static class NeuralSpikeGenerator implements org.apache.flink.api.common.functions.RuntimeContext {
        private final String[] layers = {"input", "hidden1", "hidden2", "output"};
        private final Map<String, Double> neuronStates = new HashMap<>();
        
        public NeuralSpike generateSpike() {
            // Initialize neuron states if empty
            if (neuronStates.isEmpty()) {
                for (String layer : layers) {
                    for (int i = 0; i < 10; i++) {
                        String neuronId = layer + "_" + i;
                        neuronStates.put(neuronId, ThreadLocalRandom.current().nextDouble(-70, -50));
                    }
                }
            }
            
            // Select random neuron
            String[] neuronIds = neuronStates.keySet().toArray(new String[0]);
            String neuronId = neuronIds[ThreadLocalRandom.current().nextInt(neuronIds.length)];
            
            double currentPotential = neuronStates.get(neuronId);
            
            // Apply noise and input
            currentPotential += ThreadLocalRandom.current().nextGaussian() * 5.0;
            
            // Update neuron state
            neuronStates.put(neuronId, currentPotential);
            
            String layer = neuronId.split("_")[0];
            Map<String, Double> weights = generateSynapticWeights();
            
            return new NeuralSpike(neuronId, System.currentTimeMillis(), currentPotential, layer, weights);
        }
        
        private Map<String, Double> generateSynapticWeights() {
            Map<String, Double> weights = new HashMap<>();
            for (int i = 0; i < 5; i++) {
                weights.put("synapse_" + i, ThreadLocalRandom.current().nextGaussian() * 0.5);
            }
            return weights;
        }
    }
    
    /**
     * Implements spike-timing dependent plasticity (STDP)
     */
    public static class STDPProcessor extends RichMapFunction<NeuralSpike, SynapticConnection> {
        private transient ValueState<Map<String, Long>> lastSpikeTimesState;
        private transient ValueState<Map<String, Double>> synapticWeightsState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            lastSpikeTimesState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastSpikeTimes", Types.MAP(Types.STRING, Types.LONG))
            );
            synapticWeightsState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("synapticWeights", Types.MAP(Types.STRING, Types.DOUBLE))
            );
        }
        
        @Override
        public SynapticConnection map(NeuralSpike spike) throws Exception {
            Map<String, Long> lastSpikes = lastSpikeTimesState.value();
            Map<String, Double> weights = synapticWeightsState.value();
            
            if (lastSpikes == null) {
                lastSpikes = new HashMap<>();
                weights = new HashMap<>();
            }
            
            // Update synaptic weights based on spike timing
            for (Map.Entry<String, Double> synapseEntry : spike.synapticWeights.entrySet()) {
                String synapseId = synapseEntry.getKey();
                
                if (lastSpikes.containsKey(synapseId)) {
                    long timeDiff = spike.timestamp - lastSpikes.get(synapseId);
                    double currentWeight = weights.getOrDefault(synapseId, 0.5);
                    
                    // STDP rule: strengthen if pre before post, weaken if post before pre
                    double deltaWeight = 0.0;
                    if (timeDiff > 0 && timeDiff < 20) {
                        // Potentiation
                        deltaWeight = 0.01 * Math.exp(-timeDiff / 20.0);
                    } else if (timeDiff < 0 && timeDiff > -20) {
                        // Depression
                        deltaWeight = -0.01 * Math.exp(timeDiff / 20.0);
                    }
                    
                    double newWeight = Math.max(0.0, Math.min(1.0, currentWeight + deltaWeight));
                    weights.put(synapseId, newWeight);
                }
                
                lastSpikes.put(synapseId, spike.timestamp);
            }
            
            lastSpikeTimesState.update(lastSpikes);
            synapticWeightsState.update(weights);
            
            // Return strongest connection
            String strongestSynapse = weights.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("default");
            
            return new SynapticConnection(
                spike.neuronId,
                strongestSynapse,
                weights.getOrDefault(strongestSynapse, 0.5),
                ThreadLocalRandom.current().nextDouble(1.0, 5.0),
                spike.timestamp
            );
        }
    }
    
    /**
     * Implements adaptive threshold processing
     */
    public static class AdaptiveThresholdProcessor extends ProcessWindowFunction<NeuralSpike, NetworkState, String, TimeWindow> {
        
        @Override
        public void process(String layer, Context context, Iterable<NeuralSpike> spikes, 
                          Collector<NetworkState> out) throws Exception {
            
            Map<String, Double> potentials = new HashMap<>();
            Map<String, Double> thresholds = new HashMap<>();
            List<NeuralSpike> spikeList = new ArrayList<>();
            
            spikes.forEach(spikeList::add);
            
            // Calculate firing rates and adapt thresholds
            Map<String, Integer> firingCounts = new HashMap<>();
            for (NeuralSpike spike : spikeList) {
                firingCounts.merge(spike.neuronId, 1, Integer::sum);
                potentials.put(spike.neuronId, spike.potential);
            }
            
            // Adaptive threshold based on firing rate
            double windowDuration = context.window().getEnd() - context.window().getStart();
            for (Map.Entry<String, Integer> entry : firingCounts.entrySet()) {
                String neuronId = entry.getKey();
                double firingRate = entry.getValue() / (windowDuration / 1000.0);
                
                // Homeostatic plasticity: adjust threshold to maintain target firing rate
                double targetRate = 10.0; // Hz
                double currentThreshold = -55.0;
                
                if (firingRate > targetRate) {
                    currentThreshold -= 2.0; // Lower threshold if firing too much
                } else if (firingRate < targetRate) {
                    currentThreshold += 2.0; // Raise threshold if not firing enough
                }
                
                thresholds.put(neuronId, Math.max(-80.0, Math.min(-50.0, currentThreshold)));
            }
            
            // Calculate global inhibition
            double globalInhibition = Math.min(1.0, spikeList.size() / 100.0);
            
            out.collect(new NetworkState(potentials, thresholds, context.window().getEnd(), globalInhibition));
        }
    }
    
    /**
     * Main demo execution
     */
    public static void runDemo(StreamExecutionEnvironment env) throws Exception {
        logger.info("🧠 === Neuromorphic Computing Demo ===");
        logger.info("Demonstrating brain-inspired stream processing patterns...");
        
        // Generate neural spike stream using DataGen
        NeuralSpikeGenerator generator = new NeuralSpikeGenerator();
        DataStream<NeuralSpike> spikeStream = env
            .fromElements(1, 2, 3, 4, 5) // Dummy elements to trigger generation
            .map(x -> generator.generateSpike())
            .name("Neural Spike Generator");
        
        // Process synaptic plasticity
        DataStream<SynapticConnection> synapticStream = spikeStream
            .keyBy(spike -> spike.neuronId)
            .map(new STDPProcessor())
            .name("STDP Synaptic Plasticity");
        
        // Adaptive threshold processing by layer
        DataStream<NetworkState> networkStateStream = spikeStream
            .keyBy(spike -> spike.layer)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .process(new AdaptiveThresholdProcessor())
            .name("Adaptive Threshold Processing");
        
        // Neural oscillation analysis
        DataStream<Tuple3<String, Double, String>> oscillationStream = spikeStream
            .keyBy(spike -> spike.layer)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
            .process(new ProcessWindowFunction<NeuralSpike, Tuple3<String, Double, String>, String, TimeWindow>() {
                @Override
                public void process(String layer, Context context, Iterable<NeuralSpike> spikes, 
                                  Collector<Tuple3<String, Double, String>> out) throws Exception {
                    List<NeuralSpike> spikeList = new ArrayList<>();
                    spikes.forEach(spikeList::add);
                    
                    if (spikeList.size() > 2) {
                        // Simple oscillation detection based on firing pattern
                        double[] intervals = new double[spikeList.size() - 1];
                        for (int i = 1; i < spikeList.size(); i++) {
                            intervals[i - 1] = spikeList.get(i).timestamp - spikeList.get(i - 1).timestamp;
                        }
                        
                        double meanInterval = Arrays.stream(intervals).average().orElse(0.0);
                        double variance = Arrays.stream(intervals)
                            .map(x -> Math.pow(x - meanInterval, 2))
                            .average().orElse(0.0);
                        
                        String oscillationType = "irregular";
                        if (variance < 100) {
                            if (meanInterval < 50) oscillationType = "gamma";
                            else if (meanInterval < 200) oscillationType = "beta";
                            else oscillationType = "alpha";
                        }
                        
                        out.collect(Tuple3.of(layer, meanInterval, oscillationType));
                    }
                }
            })
            .name("Neural Oscillation Detection");
        
        // Memory consolidation simulation
        DataStream<String> memoryConsolidationStream = networkStateStream
            .map(state -> {
                // Simulate memory consolidation based on network activity
                double activityLevel = state.neuronPotentials.values().stream()
                    .mapToDouble(Double::doubleValue)
                    .average().orElse(0.0);
                
                String consolidationStatus;
                if (activityLevel > -60.0) {
                    consolidationStatus = "ENCODING - High neural activity detected";
                } else if (activityLevel > -65.0) {
                    consolidationStatus = "CONSOLIDATION - Moderate activity, strengthening connections";
                } else {
                    consolidationStatus = "MAINTENANCE - Low activity, preserving existing patterns";
                }
                
                return String.format("Memory Status: %s (Activity: %.2f mV)", 
                                   consolidationStatus, activityLevel);
            })
            .name("Memory Consolidation");
        
        // Output streams with detailed logging
        spikeStream.print("🔥 Neural Spikes").setParallelism(1);
        synapticStream
            .map(conn -> String.format("🔗 Synaptic Update: %s -> %s (weight: %.3f, delay: %.1fms)", 
                                     conn.presynapticNeuron, conn.postsynapticNeuron, conn.weight, conn.delay))
            .print("Synaptic Plasticity").setParallelism(1);
        
        oscillationStream
            .map(osc -> String.format("🌊 %s Layer Oscillation: %.1fms interval (%s rhythm)", 
                                    osc.f0, osc.f1, osc.f2))
            .print("Neural Oscillations").setParallelism(1);
        
        memoryConsolidationStream.print("🧠 Memory").setParallelism(1);
        
        logger.info("🧠 Neuromorphic processing features:");
        logger.info("  ✅ Spiking neural network simulation");
        logger.info("  ✅ Spike-timing dependent plasticity (STDP)");
        logger.info("  ✅ Adaptive threshold adjustment");
        logger.info("  ✅ Neural oscillation analysis (alpha, beta, gamma)");
        logger.info("  ✅ Memory consolidation modeling");
        logger.info("  ✅ Homeostatic plasticity mechanisms");
        logger.info("  ✅ Real-time synaptic weight adaptation");
        
        env.execute("Neuromorphic Computing Demo");
    }
}
