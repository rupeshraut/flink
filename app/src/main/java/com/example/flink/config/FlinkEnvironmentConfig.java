package com.example.flink.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Flink environment configuration utility for optimized performance and reliability.
 * Provides methods to configure:
 * - State backends (HashMap, RocksDB)
 * - Checkpointing
 * - Restart strategies
 * - Parallelism
 * - Memory settings
 * - Performance optimizations
 */
public class FlinkEnvironmentConfig {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvironmentConfig.class);
    
    // Default configuration values
    private static final int DEFAULT_PARALLELISM = 4;
    private static final long DEFAULT_CHECKPOINT_INTERVAL = 60000; // 1 minute
    private static final long DEFAULT_CHECKPOINT_TIMEOUT = 300000; // 5 minutes
    private static final int DEFAULT_MAX_CONCURRENT_CHECKPOINTS = 1;
    private static final String DEFAULT_CHECKPOINT_DIR = "file:///tmp/flink-checkpoints";
    
    /**
     * Configure a Flink environment for development with optimal settings
     */
    public static StreamExecutionEnvironment createDevelopmentEnvironment() {
        LOG.info("Creating development Flink environment");
        
        Configuration config = new Configuration();
        
        // Memory configuration for development
        config.setString("taskmanager.memory.process.size", "2gb");
        config.setString("jobmanager.memory.process.size", "1gb");
        config.setString("taskmanager.memory.managed.fraction", "0.4");
        
        // Network configuration
        config.setString("taskmanager.network.memory.fraction", "0.1");
        config.setString("taskmanager.network.memory.min", "64mb");
        config.setString("taskmanager.network.memory.max", "1gb");
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        
        // Basic configuration
        env.setParallelism(DEFAULT_PARALLELISM);
        
        // Configure checkpointing for development
        configureCheckpointing(env, DEFAULT_CHECKPOINT_INTERVAL, DEFAULT_CHECKPOINT_DIR);
        
        // Configure restart strategy
        configureRestartStrategy(env, RestartStrategyType.FIXED_DELAY);
        
        // Use HashMap state backend for development (faster startup)
        configureStateBackend(env, StateBackendType.HASHMAP, DEFAULT_CHECKPOINT_DIR);
        
        LOG.info("Development environment configured with parallelism: {}", DEFAULT_PARALLELISM);
        return env;
    }
    
    /**
     * Configure a Flink environment for production with high performance settings
     */
    public static StreamExecutionEnvironment createProductionEnvironment() {
        LOG.info("Creating production Flink environment");
        
        Configuration config = new Configuration();
        
        // Memory configuration for production
        config.setString("taskmanager.memory.process.size", "8gb");
        config.setString("jobmanager.memory.process.size", "2gb");
        config.setString("taskmanager.memory.managed.fraction", "0.6");
        
        // Network configuration
        config.setString("taskmanager.network.memory.fraction", "0.15");
        config.setString("taskmanager.network.memory.min", "256mb");
        config.setString("taskmanager.network.memory.max", "2gb");
        
        // Performance optimizations
        config.setString("taskmanager.network.netty.num-arenas", "2");
        config.setString("taskmanager.network.memory.buffers-per-channel", "4");
        config.setString("taskmanager.network.memory.floating-buffers-per-gate", "8");
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        
        // Production parallelism (typically set by cluster)
        env.setParallelism(-1); // Use cluster default
        
        // Configure robust checkpointing for production
        configureCheckpointing(env, 30000, "hdfs://namenode:port/flink-checkpoints"); // 30 seconds
        
        // Configure restart strategy for production
        configureRestartStrategy(env, RestartStrategyType.EXPONENTIAL_DELAY);
        
        // Use RocksDB state backend for production (better for large state)
        configureStateBackend(env, StateBackendType.ROCKSDB, "hdfs://namenode:port/flink-checkpoints");
        
        LOG.info("Production environment configured");
        return env;
    }
    
    /**
     * Configure checkpointing with specified interval and storage
     */
    public static void configureCheckpointing(StreamExecutionEnvironment env, long intervalMs, String checkpointDir) {
        LOG.info("Configuring checkpointing with interval: {}ms, directory: {}", intervalMs, checkpointDir);
        
        env.enableCheckpointing(intervalMs);
        
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        
        // Checkpoint mode
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        
        // Checkpoint timeout
        checkpointConfig.setCheckpointTimeout(DEFAULT_CHECKPOINT_TIMEOUT);
        
        // Minimum pause between checkpoints
        checkpointConfig.setMinPauseBetweenCheckpoints(5000);
        
        // Maximum concurrent checkpoints
        checkpointConfig.setMaxConcurrentCheckpoints(DEFAULT_MAX_CONCURRENT_CHECKPOINTS);
        
        // Enable externalized checkpoints
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        // Tolerate checkpoint failures
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        
        try {
            env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkpointDir));
        } catch (Exception e) {
            LOG.warn("Could not configure checkpoint storage: {}", e.getMessage());
        }
    }
    
    /**
     * Configure restart strategy
     */
    public static void configureRestartStrategy(StreamExecutionEnvironment env, RestartStrategyType strategyType) {
        LOG.info("Configuring restart strategy: {}", strategyType);
        
        switch (strategyType) {
            case FIXED_DELAY:
                env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                        3, // Number of restart attempts
                        Time.of(10, TimeUnit.SECONDS) // Delay between restarts
                ));
                break;
                
            case EXPONENTIAL_DELAY:
                env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
                        Time.of(1, TimeUnit.SECONDS), // Initial delay
                        Time.of(60, TimeUnit.SECONDS), // Max delay
                        2.0, // Backoff multiplier
                        Time.of(10, TimeUnit.MINUTES), // Reset after
                        0.1 // Jitter
                ));
                break;
                
            case FAILURE_RATE:
                env.setRestartStrategy(RestartStrategies.failureRateRestart(
                        3, // Max failures per interval
                        Time.of(5, TimeUnit.MINUTES), // Failure rate interval
                        Time.of(10, TimeUnit.SECONDS) // Delay between restarts
                ));
                break;
                
            case NO_RESTART:
                env.setRestartStrategy(RestartStrategies.noRestart());
                break;
        }
    }
    
    /**
     * Configure state backend
     */
    public static void configureStateBackend(StreamExecutionEnvironment env, StateBackendType backendType, String checkpointDir) {
        LOG.info("Configuring state backend: {}", backendType);
        
        try {
            switch (backendType) {
                case HASHMAP:
                    env.setStateBackend(new HashMapStateBackend());
                    break;
                    
                case ROCKSDB:
                    EmbeddedRocksDBStateBackend rocksDBBackend = new EmbeddedRocksDBStateBackend(true);
                    
                    // RocksDB optimizations
                    rocksDBBackend.setPredefinedOptions(
                            org.apache.flink.contrib.streaming.state.PredefinedOptions.SPINNING_DISK_OPTIMIZED);
                    
                    env.setStateBackend(rocksDBBackend);
                    break;
            }
        } catch (Exception e) {
            LOG.error("Failed to configure state backend: {}", e.getMessage());
            // Fallback to HashMap
            env.setStateBackend(new HashMapStateBackend());
        }
    }
    
    /**
     * Configure performance optimizations
     */
    public static void configurePerformanceOptimizations(StreamExecutionEnvironment env) {
        LOG.info("Applying performance optimizations");
        
        // Buffer timeout for low-latency
        env.setBufferTimeout(100);
        
        // Configure latency tracking
        env.getConfig().setLatencyTrackingInterval(1000);
        
        // Auto watermark interval
        env.getConfig().setAutoWatermarkInterval(200);
        
        // Object reuse for better performance
        env.getConfig().enableObjectReuse();
    }
    
    // Enums for configuration options
    public enum RestartStrategyType {
        FIXED_DELAY,
        EXPONENTIAL_DELAY,
        FAILURE_RATE,
        NO_RESTART
    }
    
    public enum StateBackendType {
        HASHMAP,
        ROCKSDB
    }
}
