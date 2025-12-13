#!/usr/bin/env python3
"""
MongoDB Test Helper Script

This script helps manage MongoDB instances for testing PyMongoSQL.
"""
import subprocess
import time
import sys
import pymongo
from pymongo.errors import ServerSelectionTimeoutError


def check_docker():
    """Check if Docker is available and running"""
    try:
        print("  Checking Docker daemon...")
        result = subprocess.run(
            ["docker", "info"], capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0:
            return False, "Docker daemon is not running. Please start Docker Desktop."
        print("  Docker daemon is running")
        return True, "Docker is available"
    except subprocess.TimeoutExpired:
        return False, "Docker command timed out - Docker might be unresponsive."
    except FileNotFoundError:
        return False, "Docker is not installed or not in PATH."
    except Exception as e:
        return False, f"Docker check failed: {e}"


def start_mongodb_docker(version="8.0"):
    """Start MongoDB in Docker container"""
    print("Checking Docker availability...")
    docker_ok, docker_msg = check_docker()
    if not docker_ok:
        print(f"❌ {docker_msg}")
        print("\nAlternatives:")
        print(
            "1. Install Docker Desktop from https://www.docker.com/products/docker-desktop"
        )
        print("2. Start Docker Desktop if already installed")
        print("3. Use MongoDB Community Edition locally")
        print("4. Use MongoDB Atlas (cloud) for testing")
        return False

    print("Starting MongoDB container...")
    try:
        # Stop any existing container
        print("  Stopping existing containers...")
        subprocess.run(
            ["docker", "stop", "pymongosql-test"],
            capture_output=True,
            check=False,
            timeout=15,
        )
        subprocess.run(
            ["docker", "rm", "pymongosql-test"],
            capture_output=True,
            check=False,
            timeout=10,
        )

        # Start new container with authentication
        print(f"  Starting new MongoDB {version} container with auth...")
        result = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                "pymongosql-test",
                "-p",
                "27017:27017",
                "-e",
                "MONGO_INITDB_ROOT_USERNAME=admin",
                "-e",
                "MONGO_INITDB_ROOT_PASSWORD=secret",
                "-e",
                "MONGO_INITDB_DATABASE=test_db",
                f"mongo:{version}",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )

        print(f"Container started: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Failed to start container: {e}")
        if "permission denied" in str(e).lower():
            print("Try running PowerShell as Administrator")
        return False


def stop_mongodb_docker():
    """Stop MongoDB Docker container"""
    print("Stopping MongoDB container...")
    try:
        subprocess.run(["docker", "stop", "pymongosql-test"], check=True)
        subprocess.run(["docker", "rm", "pymongosql-test"], check=True)
        print("Container stopped and removed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Failed to stop container: {e}")
        return False


def wait_for_mongodb(host="localhost", port=27017, timeout=30):
    """Wait for MongoDB to be ready"""
    print(f"Waiting for MongoDB at {host}:{port}... (timeout: {timeout}s)")

    start_time = time.time()
    attempt = 0
    while time.time() - start_time < timeout:
        attempt += 1
        try:
            client = pymongo.MongoClient(host, port, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            print(f"\nMongoDB is ready! (attempt {attempt})")
            client.close()
            return True
        except ServerSelectionTimeoutError:
            elapsed = int(time.time() - start_time)
            print(f"\r  Attempt {attempt} (elapsed: {elapsed}s)...", end="", flush=True)
            time.sleep(2)
        except Exception as e:
            print(f"\n  Connection error: {e}")
            time.sleep(2)

    print(f"\n❌ Timeout waiting for MongoDB after {timeout}s")
    return False


def setup_test_data():
    """Setup test data in MongoDB"""
    print("Setting up test data...")
    try:
        # Connect with authentication
        client = pymongo.MongoClient(
            "localhost", 27017, username="admin", password="secret", authSource="admin"
        )
        db = client.test_db

        # Clear existing data
        db.users.drop()
        db.products.drop()

        # Insert test data
        db.users.insert_many(
            [
                {
                    "_id": "1",
                    "name": "John Doe",
                    "age": 30,
                    "email": "john@example.com",
                },
                {
                    "_id": "2",
                    "name": "Jane Smith",
                    "age": 25,
                    "email": "jane@example.com",
                },
                {
                    "_id": "3",
                    "name": "Bob Johnson",
                    "age": 35,
                    "email": "bob@example.com",
                },
            ]
        )

        db.products.insert_many(
            [
                {
                    "_id": "p1",
                    "name": "Laptop",
                    "price": 1000,
                    "category": "Electronics",
                },
                {"_id": "p2", "name": "Mouse", "price": 25, "category": "Electronics"},
                {"_id": "p3", "name": "Book", "price": 15, "category": "Education"},
            ]
        )

        print(f"Inserted {db.users.count_documents({})} users")
        print(f"Inserted {db.products.count_documents({})} products")
        return True

    except Exception as e:
        print(f"Failed to setup test data: {e}")
        return False


def suggest_alternatives():
    """Suggest alternative MongoDB installation methods"""
    print("\n📋 MongoDB Installation Alternatives:")
    print("\n1. MongoDB Community Edition (Local):")
    print("   - Download from: https://www.mongodb.com/try/download/community")
    print("   - Install and start as Windows service")
    print("   - Default connection: mongodb://localhost:27017")

    print("\n2. MongoDB Atlas (Cloud):")
    print("   - Free tier available at: https://www.mongodb.com/cloud/atlas")
    print("   - No local installation required")
    print("   - Get connection string from Atlas dashboard")

    print("\n3. Docker Desktop:")
    print("   - Install from: https://www.docker.com/products/docker-desktop")
    print("   - Start Docker Desktop")
    print("   - Run this script again")

    print("\n4. Continue without MongoDB (limited testing):")
    print("   - Only basic unit tests will work")
    print("   - Database integration tests will be skipped")


def main():
    """Main function"""
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print(
            "Usage: python run_test_server.py [start|stop|setup|status|alternatives|test] [version]"
        )
        print("Version examples: 7.0, 8.0 (default: 8.0)")
        sys.exit(1)

    command = sys.argv[1]
    version = sys.argv[2] if len(sys.argv) == 3 else "8.0"

    if command == "start":
        if start_mongodb_docker(version):
            if wait_for_mongodb():
                setup_test_data()
                print(f"\n✅ MongoDB {version} test instance is ready!")
                print("Connection: mongodb://localhost:27017/test_db")
            else:
                print("❌ MongoDB failed to start properly")

    elif command == "stop":
        stop_mongodb_docker()
        print("✅ MongoDB test instance stopped")

    elif command == "setup":
        if wait_for_mongodb():
            setup_test_data()
            print("✅ Test data setup complete")
        else:
            print("❌ MongoDB is not running")

    elif command == "status":
        if wait_for_mongodb(timeout=5):
            print("✅ MongoDB is running and accessible")
            try:
                client = pymongo.MongoClient(
                    "localhost",
                    27017,
                    username="admin",
                    password="secret",
                    authSource="admin",
                )
                db = client.test_db
                print(f"Users: {db.users.count_documents({})}")
                print(f"Products: {db.products.count_documents({})}")
            except Exception as e:
                print(f"Auth connection failed: {e}")
                print("Trying without auth...")
                client = pymongo.MongoClient("localhost", 27017)
                db = client.test_db
                print(f"Users: {db.users.count_documents({})}")
                print(f"Products: {db.products.count_documents({})}")
        else:
            print("❌ MongoDB is not accessible")

    elif command == "alternatives":
        suggest_alternatives()

    elif command == "test":
        print("🔍 Testing Docker availability...")
        docker_ok, docker_msg = check_docker()
        print(f"Result: {docker_msg}")
        if docker_ok:
            print("✅ Docker is working - try 'start' command")
        else:
            print("❌ Docker issue detected")

    else:
        print("Invalid command. Use: start, stop, setup, status, alternatives, or test")


if __name__ == "__main__":
    main()
