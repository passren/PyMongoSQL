#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MongoDB Test Helper Script

This script helps manage MongoDB instances for testing PyMongoSQL.
"""
import json
import os
import subprocess
import sys
import time

import pymongo
from pymongo.errors import ServerSelectionTimeoutError


def load_config():
    """Load configuration from JSON file"""
    config_file = os.path.join(os.path.dirname(__file__), "server_config.json")
    try:
        with open(config_file, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file {config_file} not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in configuration file: {e}")
        sys.exit(1)


# Load configuration
config = load_config()

# MongoDB Configuration
MONGODB_HOST = config["mongodb"]["host"]
MONGODB_PORT = config["mongodb"]["port"]
MONGODB_DATABASE = config["mongodb"]["database"]

# Admin User (for container initialization and user management)
ADMIN_USERNAME = config["admin_user"]["username"]
ADMIN_PASSWORD = config["admin_user"]["password"]
ADMIN_AUTH_SOURCE = config["admin_user"]["auth_source"]

# Test User (for application connections)
TEST_USERNAME = config["test_user"]["username"]
TEST_PASSWORD = config["test_user"]["password"]
TEST_AUTH_SOURCE = config["test_user"]["auth_source"]

# Container Configuration
CONTAINER_NAME = config["container"]["name"]

# Test Data Configuration
TEST_DATA_FILES = config.get("test_data_files", {})
# Backward compatibility
if "test_data_file" in config and not TEST_DATA_FILES:
    TEST_DATA_FILES = {"legacy": config["test_data_file"]}


def check_docker():
    """Check if Docker is available and running"""
    try:
        print("  Checking Docker daemon...")
        result = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=10)
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
        print(f"[ERROR] {docker_msg}")
        print("\nAlternatives:")
        print("1. Install Docker Desktop from https://www.docker.com/products/docker-desktop")
        print("2. Start Docker Desktop if already installed")
        print("3. Use MongoDB Community Edition locally")
        print("4. Use MongoDB Atlas (cloud) for testing")
        return False

    print("Starting MongoDB container...")
    try:
        # Stop any existing container
        print("  Stopping existing containers...")
        subprocess.run(
            ["docker", "stop", CONTAINER_NAME],
            capture_output=True,
            check=False,
            timeout=15,
        )
        subprocess.run(
            ["docker", "rm", CONTAINER_NAME],
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
                CONTAINER_NAME,
                "-p",
                f"{MONGODB_PORT}:{MONGODB_PORT}",
                "-e",
                f"MONGO_INITDB_ROOT_USERNAME={ADMIN_USERNAME}",
                "-e",
                f"MONGO_INITDB_ROOT_PASSWORD={ADMIN_PASSWORD}",
                "-e",
                f"MONGO_INITDB_DATABASE={MONGODB_DATABASE}",
                f"mongo:{version}",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=120,
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
        subprocess.run(["docker", "stop", CONTAINER_NAME], check=True)
        subprocess.run(["docker", "rm", CONTAINER_NAME], check=True)
        print("Container stopped and removed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Failed to stop container: {e}")
        return False


def wait_for_mongodb(host=MONGODB_HOST, port=MONGODB_PORT, timeout=90):
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

    print(f"\n[ERROR] Timeout waiting for MongoDB after {timeout}s")
    return False


def create_database_user():
    """Create a user for test_db database"""
    print("Creating database user...")
    try:
        # Connect as admin to create user
        admin_client = pymongo.MongoClient(
            MONGODB_HOST, MONGODB_PORT, username=ADMIN_USERNAME, password=ADMIN_PASSWORD, authSource=ADMIN_AUTH_SOURCE
        )

        # Create user for database
        admin_client[MONGODB_DATABASE].command(
            "createUser",
            TEST_USERNAME,
            pwd=TEST_PASSWORD,
            roles=[
                {"role": "readWrite", "db": MONGODB_DATABASE},
                {"role": "dbAdmin", "db": MONGODB_DATABASE},
            ],
        )
        print(f"Database user '{TEST_USERNAME}' created successfully")
        admin_client.close()
        return True
    except pymongo.errors.DuplicateKeyError:
        print(f"Database user '{TEST_USERNAME}' already exists")
        return True
    except Exception as e:
        print(f"Failed to create database user: {e}")
        return False


def load_test_data():
    """Load test data from JSON files"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    test_data = {}

    # Handle legacy single file format for backward compatibility
    if "legacy" in TEST_DATA_FILES:
        test_data_path = os.path.join(script_dir, TEST_DATA_FILES["legacy"])
        try:
            with open(test_data_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"[ERROR] Test data file not found: {test_data_path}")
            return None
        except json.JSONDecodeError as e:
            print(f"[ERROR] Invalid JSON in test data file: {e}")
            return None

    # Handle new multiple files format
    for collection_name, file_path in TEST_DATA_FILES.items():
        full_path = os.path.join(script_dir, file_path)
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                test_data[collection_name] = json.load(f)
                print(f"  Loaded {len(test_data[collection_name])} {collection_name}")
        except FileNotFoundError:
            print(f"[ERROR] Test data file not found: {full_path}")
            return None
        except json.JSONDecodeError as e:
            print(f"[ERROR] Invalid JSON in {collection_name} file: {e}")
            return None

    return test_data


def setup_test_data():
    """Setup test data in MongoDB"""
    print("Setting up test data...")

    # Load test data from files
    test_data = load_test_data()
    if test_data is None:
        return False

    try:
        # Connect with database user
        client = pymongo.MongoClient(
            MONGODB_HOST,
            MONGODB_PORT,
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            authSource=TEST_AUTH_SOURCE,
        )
        db = client[MONGODB_DATABASE]

        # List of all collections to handle
        collections = ["users", "products", "categories", "orders", "analytics", "departments", "suppliers"]

        # Clear existing data and insert new data for each collection
        for collection_name in collections:
            if collection_name in test_data and test_data[collection_name]:
                # Drop existing collection
                db[collection_name].drop()

                # Insert new data
                db[collection_name].insert_many(test_data[collection_name])
                count = db[collection_name].count_documents({})
                print(f"  Inserted {count} {collection_name}")

        print("[SUCCESS] Test data setup completed successfully!")
        return True

    except Exception as e:
        print(f"Failed to setup test data: {e}")
        return False


def suggest_alternatives():
    """Suggest alternative MongoDB installation methods"""
    print("\n[INFO] MongoDB Installation Alternatives:")
    print("\n1. MongoDB Community Edition (Local):")
    print("   - Download from: https://www.mongodb.com/try/download/community")
    print("   - Install and start as Windows service")
    print(f"   - Default connection: mongodb://{MONGODB_HOST}:{MONGODB_PORT}")

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
        print("Usage: python run_test_server.py [start|stop|setup|status|alternatives|test] [version]")
        print("Version examples: 7.0, 8.0 (default: 8.0)")
        sys.exit(1)

    command = sys.argv[1]
    version = sys.argv[2] if len(sys.argv) == 3 else "8.0"

    if command == "start":
        if start_mongodb_docker(version):
            if wait_for_mongodb():
                if create_database_user():
                    setup_test_data()
                    print(f"\n[SUCCESS] MongoDB {version} test instance is ready!")
                    print(
                        f"Connection: mongodb://{TEST_USERNAME}:{TEST_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}/{MONGODB_DATABASE}?authSource={TEST_AUTH_SOURCE}"  # noqa: E501
                    )
                else:
                    print("[ERROR] Failed to create database user")
            else:
                print("[ERROR] MongoDB failed to start properly")

    elif command == "stop":
        stop_mongodb_docker()
        print("[SUCCESS] MongoDB test instance stopped")

    elif command == "setup":
        if wait_for_mongodb():
            create_database_user()
            setup_test_data()
            print("[SUCCESS] Test data setup complete")
        else:
            print("[ERROR] MongoDB is not running")

    elif command == "status":
        if wait_for_mongodb(timeout=5):
            print("[SUCCESS] MongoDB is running and accessible")
            try:
                client = pymongo.MongoClient(
                    MONGODB_HOST,
                    MONGODB_PORT,
                    username=TEST_USERNAME,
                    password=TEST_PASSWORD,
                    authSource=TEST_AUTH_SOURCE,
                )
                db = client[MONGODB_DATABASE]
                print(f"Users: {db.users.count_documents({})}")
                print(f"Products: {db.products.count_documents({})}")
            except Exception as e:
                print(f"Database user connection failed: {e}")
                print("Trying to create user and retry...")
                if create_database_user():
                    client = pymongo.MongoClient(
                        MONGODB_HOST,
                        MONGODB_PORT,
                        username=TEST_USERNAME,
                        password=TEST_PASSWORD,
                        authSource=TEST_AUTH_SOURCE,
                    )
                    db = client[MONGODB_DATABASE]
                    print(f"Users: {db.users.count_documents({})}")
                    print(f"Products: {db.products.count_documents({})}")
                else:
                    print("Failed to create user, trying without auth...")
                    client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)
                    db = client[MONGODB_DATABASE]
                    print(f"Users: {db.users.count_documents({})}")
                    print(f"Products: {db.products.count_documents({})}")
                    print("[WARNING] Using unauthenticated connection")
        else:
            print("[ERROR] MongoDB is not accessible")

    elif command == "alternatives":
        suggest_alternatives()

    elif command == "test":
        print("[TESTING] Docker availability...")
        docker_ok, docker_msg = check_docker()
        print(f"Result: {docker_msg}")
        if docker_ok:
            print("[SUCCESS] Docker is working - try 'start' command")
        else:
            print("[ERROR] Docker issue detected")

    else:
        print("Invalid command. Use: start, stop, setup, status, alternatives, or test")


if __name__ == "__main__":
    main()
