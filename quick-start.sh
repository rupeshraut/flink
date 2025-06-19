#!/bin/bash

# Apache Flink Advanced Use Cases - Quick Start Script
# This script sets up the complete development environment

set -e

echo "🚀 Apache Flink Advanced Use Cases - Quick Start"
echo "================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check Java
    if command -v java &> /dev/null; then
        JAVA_VERSION=$(java -version 2>&1 | grep -oP 'version "?\K[0-9.]+')
        if [[ $(echo "$JAVA_VERSION" | cut -d. -f1) -ge 17 ]]; then
            print_status "Java $JAVA_VERSION found ✓"
        else
            print_error "Java 17 or higher required. Found: $JAVA_VERSION"
            exit 1
        fi
    else
        print_error "Java not found. Please install Java 17 or higher."
        exit 1
    fi
    
    # Check Gradle
    if command -v ./gradlew &> /dev/null; then
        print_status "Gradle wrapper found ✓"
    else
        print_error "Gradle wrapper not found. Please ensure you're in the project root."
        exit 1
    fi
    
    # Check Docker (optional)
    if command -v docker &> /dev/null; then
        print_status "Docker found ✓"
        DOCKER_AVAILABLE=true
    else
        print_warning "Docker not found. Some features may not be available."
        DOCKER_AVAILABLE=false
    fi
    
    # Check memory
    if [[ $(free -g | awk '/^Mem:/{print $2}') -ge 4 ]]; then
        print_status "Sufficient memory available (4GB+) ✓"
    else
        print_warning "Less than 4GB memory available. Performance may be impacted."
    fi
}

# Build the project
build_project() {
    print_header "Building Project"
    
    print_status "Running Gradle build..."
    ./gradlew clean build -x test
    
    if [ $? -eq 0 ]; then
        print_status "Build completed successfully ✓"
    else
        print_error "Build failed. Please check the output above."
        exit 1
    fi
}

# Start infrastructure
start_infrastructure() {
    if [ "$DOCKER_AVAILABLE" = true ]; then
        print_header "Starting Infrastructure"
        
        print_status "Starting Kafka and supporting services..."
        docker-compose up -d zookeeper kafka kafka-ui
        
        # Wait for Kafka to be ready
        print_status "Waiting for Kafka to be ready..."
        sleep 30
        
        # Check if Kafka is running
        if docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &> /dev/null; then
            print_status "Kafka is ready ✓"
        else
            print_warning "Kafka may not be fully ready. You may need to wait a bit longer."
        fi
        
        # Start Flink cluster (optional)
        read -p "Do you want to start Flink cluster in Docker? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            print_status "Starting Flink cluster..."
            docker-compose up -d flink-jobmanager flink-taskmanager
            sleep 15
            print_status "Flink cluster started. Web UI available at http://localhost:8081"
        fi
        
        print_status "Infrastructure services started:"
        print_status "  - Kafka UI: http://localhost:8080"
        print_status "  - Flink UI: http://localhost:8081 (if started)"
        
    else
        print_warning "Docker not available. Please start Kafka manually."
        print_status "You can download Kafka from: https://kafka.apache.org/downloads"
    fi
}

# Show demo menu
show_demo_menu() {
    print_header "Available Demonstrations"
    
    echo "1. Kafka Stream Processing    - Event streaming with Kafka integration"
    echo "2. Complex Event Processing   - Pattern detection and fraud analysis"
    echo "3. Real-time Analytics       - Live metrics and KPI calculations"
    echo "4. Advanced Windowing        - Multiple windowing strategies"
    echo "5. Stateful Processing       - State management and recovery"
    echo "6. Flink SQL Analytics       - Stream processing with SQL"
    echo "7. All Demos (Interactive)   - Show menu of all demos"
    echo "8. Exit"
    echo
    
    read -p "Select a demo to run (1-8): " choice
    
    case $choice in
        1)
            print_status "Running Kafka Stream Processing Demo..."
            ./gradlew run --args='kafka'
            ;;
        2)
            print_status "Running Complex Event Processing Demo..."
            ./gradlew run --args='cep'
            ;;
        3)
            print_status "Running Real-time Analytics Demo..."
            ./gradlew run --args='analytics'
            ;;
        4)
            print_status "Running Advanced Windowing Demo..."
            ./gradlew run --args='windowing'
            ;;
        5)
            print_status "Running Stateful Processing Demo..."
            ./gradlew run --args='stateful'
            ;;
        6)
            print_status "Running Flink SQL Analytics Demo..."
            ./gradlew run --args='sql'
            ;;
        7)
            print_status "Running All Demos (Interactive)..."
            ./gradlew run
            ;;
        8)
            print_status "Goodbye! 👋"
            exit 0
            ;;
        *)
            print_error "Invalid choice. Please select 1-8."
            show_demo_menu
            ;;
    esac
}

# Cleanup function
cleanup() {
    print_header "Cleanup"
    
    if [ "$DOCKER_AVAILABLE" = true ]; then
        read -p "Do you want to stop Docker services? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            print_status "Stopping Docker services..."
            docker-compose down
        fi
    fi
    
    print_status "Cleanup completed."
}

# Main execution
main() {
    echo "Welcome to Apache Flink Advanced Use Cases!"
    echo "This script will help you set up and run the demonstrations."
    echo
    
    # Parse command line arguments
    case "${1:-}" in
        --build-only)
            check_prerequisites
            build_project
            exit 0
            ;;
        --infrastructure-only)
            check_prerequisites
            start_infrastructure
            exit 0
            ;;
        --cleanup)
            cleanup
            exit 0
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo
            echo "Options:"
            echo "  --build-only         Only build the project"
            echo "  --infrastructure-only Only start infrastructure"
            echo "  --cleanup           Stop services and cleanup"
            echo "  --help, -h          Show this help message"
            echo
            echo "Without options, runs the full setup and demo menu."
            exit 0
            ;;
    esac
    
    # Full setup
    check_prerequisites
    build_project
    start_infrastructure
    
    echo
    print_status "Setup completed! 🎉"
    echo
    
    # Show demo menu
    while true; do
        show_demo_menu
        echo
        read -p "Do you want to run another demo? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            break
        fi
    done
    
    print_status "Thank you for trying Apache Flink Advanced Use Cases!"
    
    # Ask about cleanup
    echo
    read -p "Would you like to clean up Docker services? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cleanup
    fi
}

# Set up signal handlers
trap cleanup EXIT INT TERM

# Run main function
main "$@"
