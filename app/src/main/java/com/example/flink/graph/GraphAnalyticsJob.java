package com.example.flink.graph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.gelly.Edge;
import org.apache.flink.gelly.Graph;
import org.apache.flink.gelly.Vertex;
import org.apache.flink.gelly.algorithm.PageRank;
import org.apache.flink.gelly.algorithm.ConnectedComponents;
import org.apache.flink.gelly.algorithm.TriangleEnumerator;
import org.apache.flink.gelly.algorithm.clustering.ClusteringCoefficient;
import org.apache.flink.gelly.algorithm.shortestpaths.SingleSourceShortestPaths;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.jgrapht.alg.clustering.KSpanningTreeClustering;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Advanced Graph Analytics with Apache Flink
 * 
 * Demonstrates:
 * - Real-time graph construction from streaming data
 * - Social network analysis (PageRank, clustering coefficient)
 * - Community detection and network clustering
 * - Fraud detection using graph patterns
 * - Dynamic graph evolution tracking
 * - Anomaly detection in graph structures
 */
public class GraphAnalyticsJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(GraphAnalyticsJob.class);
    
    public static void runDemo(StreamExecutionEnvironment env) throws Exception {
        LOG.info("🕸️ Starting Advanced Graph Analytics Demo");
        
        // Generate synthetic social network data
        DataStream<SocialInteraction> interactions = generateSocialNetworkStream(env);
        
        // Demo 1: Real-time Graph Construction and PageRank
        runRealTimePageRank(interactions);
        
        // Demo 2: Community Detection
        runCommunityDetection(interactions);
        
        // Demo 3: Fraud Detection with Graph Patterns
        runFraudDetection(interactions);
        
        // Demo 4: Network Evolution Analysis
        runNetworkEvolution(interactions);
        
        LOG.info("✅ Graph Analytics demo configured - executing...");
        env.execute("Advanced Graph Analytics Job");
    }
    
    /**
     * Generate synthetic social network interaction data
     */
    private static DataStream<SocialInteraction> generateSocialNetworkStream(StreamExecutionEnvironment env) {
        return env.addSource(new SocialNetworkDataGenerator())
                  .name("Social Network Data Generator");
    }
    
    /**
     * Real-time PageRank calculation on dynamic graphs
     */
    private static void runRealTimePageRank(DataStream<SocialInteraction> interactions) {
        LOG.info("📊 Running Real-time PageRank Analysis");
        
        // Create edges from interactions
        DataStream<Edge<Long, Double>> edges = interactions
            .map(interaction -> new Edge<>(
                (long) interaction.fromUser, 
                (long) interaction.toUser, 
                interaction.weight))
            .name("Convert to Graph Edges");
        
        // Window-based graph construction and PageRank
        edges.keyBy(Edge::getSource)
             .window(TumblingEventTimeWindows.of(Time.minutes(5)))
             .process(new PageRankProcessor())
             .name("Windowed PageRank Analysis")
             .print("PageRank Results");
    }
    
    /**
     * Community detection using clustering algorithms
     */
    private static void runCommunityDetection(DataStream<SocialInteraction> interactions) {
        LOG.info("🏘️ Running Community Detection Analysis");
        
        interactions
            .keyBy(SocialInteraction::getFromUser)
            .window(TumblingEventTimeWindows.of(Time.minutes(10)))
            .process(new CommunityDetectionProcessor())
            .name("Community Detection")
            .print("Communities Detected");
    }
    
    /**
     * Fraud detection using graph pattern analysis
     */
    private static void runFraudDetection(DataStream<SocialInteraction> interactions) {
        LOG.info("🚨 Running Graph-based Fraud Detection");
        
        interactions
            .filter(new SuspiciousActivityFilter())
            .keyBy(SocialInteraction::getFromUser)
            .window(TumblingEventTimeWindows.of(Time.minutes(2)))
            .process(new FraudDetectionProcessor())
            .name("Fraud Pattern Detection")
            .print("Fraud Alerts");
    }
    
    /**
     * Network evolution and structural change analysis
     */
    private static void runNetworkEvolution(DataStream<SocialInteraction> interactions) {
        LOG.info("📈 Running Network Evolution Analysis");
        
        interactions
            .window(TumblingEventTimeWindows.of(Time.minutes(3)))
            .process(new NetworkEvolutionProcessor())
            .name("Network Evolution Tracking")
            .print("Network Metrics");
    }
    
    /**
     * Social interaction data model
     */
    public static class SocialInteraction {
        public int fromUser;
        public int toUser;
        public String interactionType; // like, share, comment, message
        public double weight;
        public long timestamp;
        public Map<String, Object> metadata;
        
        public SocialInteraction() {}
        
        public SocialInteraction(int fromUser, int toUser, String interactionType, 
                               double weight, long timestamp) {
            this.fromUser = fromUser;
            this.toUser = toUser;
            this.interactionType = interactionType;
            this.weight = weight;
            this.timestamp = timestamp;
            this.metadata = new HashMap<>();
        }
        
        public int getFromUser() { return fromUser; }
        public int getToUser() { return toUser; }
        public String getInteractionType() { return interactionType; }
        public double getWeight() { return weight; }
        public long getTimestamp() { return timestamp; }
        
        @Override
        public String toString() {
            return String.format("SocialInteraction{%d->%d, type='%s', weight=%.2f, time=%d}", 
                               fromUser, toUser, interactionType, weight, timestamp);
        }
    }
    
    /**
     * PageRank processor for windowed graph analysis
     */
    public static class PageRankProcessor extends ProcessWindowFunction<Edge<Long, Double>, 
                                                                     Tuple2<Long, Double>, 
                                                                     Long, TimeWindow> {
        @Override
        public void process(Long key, Context context, 
                          Iterable<Edge<Long, Double>> edges, 
                          Collector<Tuple2<Long, Double>> out) throws Exception {
            
            // Build graph from edges in window
            Set<Long> vertices = new HashSet<>();
            List<Edge<Long, Double>> edgeList = new ArrayList<>();
            
            for (Edge<Long, Double> edge : edges) {
                vertices.add(edge.getSource());
                vertices.add(edge.getTarget());
                edgeList.add(edge);
            }
            
            if (vertices.size() < 2) return;
            
            // Simple PageRank simulation (in real implementation, use Flink Gelly)
            Map<Long, Double> pageRank = calculateSimplePageRank(vertices, edgeList);
            
            // Output top ranked nodes
            pageRank.entrySet().stream()
                    .sorted(Map.Entry.<Long, Double>comparingByValue().reversed())
                    .limit(10)
                    .forEach(entry -> out.collect(new Tuple2<>(entry.getKey(), entry.getValue())));
        }
        
        private Map<Long, Double> calculateSimplePageRank(Set<Long> vertices, 
                                                         List<Edge<Long, Double>> edges) {
            Map<Long, Double> ranks = new HashMap<>();
            Map<Long, List<Long>> outLinks = new HashMap<>();
            
            // Initialize
            for (Long vertex : vertices) {
                ranks.put(vertex, 1.0 / vertices.size());
                outLinks.put(vertex, new ArrayList<>());
            }
            
            // Build adjacency list
            for (Edge<Long, Double> edge : edges) {
                outLinks.get(edge.getSource()).add(edge.getTarget());
            }
            
            // Simple PageRank iterations
            for (int i = 0; i < 10; i++) {
                Map<Long, Double> newRanks = new HashMap<>();
                for (Long vertex : vertices) {
                    newRanks.put(vertex, 0.15 / vertices.size());
                }
                
                for (Long vertex : vertices) {
                    List<Long> links = outLinks.get(vertex);
                    if (!links.isEmpty()) {
                        double contribution = 0.85 * ranks.get(vertex) / links.size();
                        for (Long target : links) {
                            newRanks.put(target, newRanks.get(target) + contribution);
                        }
                    }
                }
                ranks = newRanks;
            }
            
            return ranks;
        }
    }
    
    /**
     * Community detection processor
     */
    public static class CommunityDetectionProcessor extends ProcessWindowFunction<SocialInteraction, 
                                                                                String, 
                                                                                Integer, TimeWindow> {
        @Override
        public void process(Integer key, Context context, 
                          Iterable<SocialInteraction> interactions, 
                          Collector<String> out) throws Exception {
            
            Map<Integer, Set<Integer>> adjacency = new HashMap<>();
            Set<Integer> allUsers = new HashSet<>();
            
            // Build adjacency map
            for (SocialInteraction interaction : interactions) {
                allUsers.add(interaction.fromUser);
                allUsers.add(interaction.toUser);
                
                adjacency.computeIfAbsent(interaction.fromUser, k -> new HashSet<>())
                         .add(interaction.toUser);
                adjacency.computeIfAbsent(interaction.toUser, k -> new HashSet<>())
                         .add(interaction.fromUser);
            }
            
            // Simple community detection using connected components
            Set<Integer> visited = new HashSet<>();
            int communityId = 0;
            
            for (Integer user : allUsers) {
                if (!visited.contains(user)) {
                    Set<Integer> community = new HashSet<>();
                    dfs(user, adjacency, visited, community);
                    
                    if (community.size() > 1) {
                        out.collect(String.format("Community %d: %s (size: %d)", 
                                                 communityId++, community, community.size()));
                    }
                }
            }
        }
        
        private void dfs(Integer user, Map<Integer, Set<Integer>> adjacency, 
                        Set<Integer> visited, Set<Integer> community) {
            if (visited.contains(user)) return;
            
            visited.add(user);
            community.add(user);
            
            Set<Integer> neighbors = adjacency.get(user);
            if (neighbors != null) {
                for (Integer neighbor : neighbors) {
                    dfs(neighbor, adjacency, visited, community);
                }
            }
        }
    }
    
    /**
     * Filter for suspicious activities
     */
    public static class SuspiciousActivityFilter implements FilterFunction<SocialInteraction> {
        @Override
        public boolean filter(SocialInteraction interaction) throws Exception {
            // Flag suspicious patterns: rapid interactions, unusual weights
            return interaction.weight > 5.0 || 
                   interaction.interactionType.equals("spam") ||
                   (interaction.timestamp % 1000) < 100; // Rapid succession
        }
    }
    
    /**
     * Fraud detection processor using graph patterns
     */
    public static class FraudDetectionProcessor extends ProcessWindowFunction<SocialInteraction, 
                                                                           String, 
                                                                           Integer, TimeWindow> {
        @Override
        public void process(Integer key, Context context, 
                          Iterable<SocialInteraction> interactions, 
                          Collector<String> out) throws Exception {
            
            Map<String, Integer> patternCounts = new HashMap<>();
            List<SocialInteraction> interactionList = new ArrayList<>();
            
            for (SocialInteraction interaction : interactions) {
                interactionList.add(interaction);
            }
            
            // Detect fraud patterns
            if (detectBurstPattern(interactionList)) {
                patternCounts.put("BURST_ACTIVITY", patternCounts.getOrDefault("BURST_ACTIVITY", 0) + 1);
            }
            
            if (detectFakeAccountPattern(interactionList)) {
                patternCounts.put("FAKE_ACCOUNT", patternCounts.getOrDefault("FAKE_ACCOUNT", 0) + 1);
            }
            
            if (detectSpamPattern(interactionList)) {
                patternCounts.put("SPAM_BEHAVIOR", patternCounts.getOrDefault("SPAM_BEHAVIOR", 0) + 1);
            }
            
            // Output fraud alerts
            for (Map.Entry<String, Integer> entry : patternCounts.entrySet()) {
                if (entry.getValue() > 0) {
                    out.collect(String.format("🚨 FRAUD ALERT - User %d: %s detected (%d instances)", 
                                             key, entry.getKey(), entry.getValue()));
                }
            }
        }
        
        private boolean detectBurstPattern(List<SocialInteraction> interactions) {
            // Detect if user has too many interactions in short time
            return interactions.size() > 10;
        }
        
        private boolean detectFakeAccountPattern(List<SocialInteraction> interactions) {
            // Detect repetitive interaction patterns
            Set<Integer> targets = new HashSet<>();
            for (SocialInteraction interaction : interactions) {
                targets.add(interaction.toUser);
            }
            return targets.size() < interactions.size() * 0.3; // Low diversity
        }
        
        private boolean detectSpamPattern(List<SocialInteraction> interactions) {
            // Detect spam-like behavior
            long spamCount = interactions.stream()
                                       .filter(i -> i.interactionType.equals("spam"))
                                       .count();
            return spamCount > interactions.size() * 0.5;
        }
    }
    
    /**
     * Network evolution analysis processor
     */
    public static class NetworkEvolutionProcessor extends ProcessWindowFunction<SocialInteraction, 
                                                                              String, 
                                                                              String, TimeWindow> {
        @Override
        public void process(String key, Context context, 
                          Iterable<SocialInteraction> interactions, 
                          Collector<String> out) throws Exception {
            
            Set<Integer> users = new HashSet<>();
            Set<String> edges = new HashSet<>();
            Map<String, Integer> interactionTypes = new HashMap<>();
            double totalWeight = 0.0;
            int count = 0;
            
            for (SocialInteraction interaction : interactions) {
                users.add(interaction.fromUser);
                users.add(interaction.toUser);
                edges.add(interaction.fromUser + "->" + interaction.toUser);
                
                interactionTypes.put(interaction.interactionType, 
                                   interactionTypes.getOrDefault(interaction.interactionType, 0) + 1);
                
                totalWeight += interaction.weight;
                count++;
            }
            
            // Calculate network metrics
            double density = edges.size() / (double) (users.size() * (users.size() - 1));
            double avgWeight = count > 0 ? totalWeight / count : 0.0;
            
            // Output network evolution metrics
            out.collect(String.format(
                "📊 Network Metrics [%s]: Users=%d, Edges=%d, Density=%.4f, AvgWeight=%.2f, Types=%s",
                context.window(), users.size(), edges.size(), density, avgWeight, interactionTypes
            ));
        }
    }
    
    /**
     * Social network data generator
     */
    public static class SocialNetworkDataGenerator extends org.apache.flink.streaming.api.functions.source.SourceFunction<SocialInteraction> {
        private volatile boolean running = true;
        private final Random random = new Random();
        private final String[] interactionTypes = {"like", "share", "comment", "message", "follow", "spam"};
        
        @Override
        public void run(SourceContext<SocialInteraction> ctx) throws Exception {
            while (running) {
                // Generate realistic social network interactions
                int fromUser = random.nextInt(1000) + 1;
                int toUser = random.nextInt(1000) + 1;
                
                if (fromUser != toUser) {
                    String type = interactionTypes[random.nextInt(interactionTypes.length)];
                    double weight = generateWeight(type);
                    long timestamp = System.currentTimeMillis();
                    
                    SocialInteraction interaction = new SocialInteraction(
                        fromUser, toUser, type, weight, timestamp
                    );
                    
                    ctx.collect(interaction);
                }
                
                // Control generation rate
                Thread.sleep(random.nextInt(100) + 50);
            }
        }
        
        private double generateWeight(String type) {
            switch (type) {
                case "like": return random.nextDouble() * 2 + 1;
                case "share": return random.nextDouble() * 3 + 2;
                case "comment": return random.nextDouble() * 4 + 3;
                case "message": return random.nextDouble() * 5 + 4;
                case "follow": return random.nextDouble() * 6 + 5;
                case "spam": return random.nextDouble() * 10 + 8; // High weight for spam
                default: return 1.0;
            }
        }
        
        @Override
        public void cancel() {
            running = false;
        }
    }
}
