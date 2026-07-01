#!/usr/bin/env python3
"""
Comprehensive test runner for Reddit Orchestrator.
Runs all test suites and provides a summary.
"""

import unittest
import sys
import os
from datetime import datetime


def discover_and_run_tests():
    """Discover and run all tests in the project."""
    
    # Create a test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test files
    test_files = [
        'test_models.py',
        'test_extractor.py', 
        'test_kafka_producer.py',
        'test_kafka_consumer.py',
        'test_pgvector_writer.py',
        'test_neo4j_writer.py'
    ]
    
    # Try to add test_app.py if Flask is available
    try:
        import flask
        test_files.append('test_app.py')
    except ImportError:
        print("Flask not available, skipping app tests")
    
    # Load tests from each file
    for test_file in test_files:
        if os.path.exists(test_file):
            try:
                tests = loader.discover('.', pattern=test_file.replace('.py', '.py'))
                suite.addTests(tests)
                print(f"✓ Loaded tests from {test_file}")
            except Exception as e:
                print(f"✗ Failed to load tests from {test_file}: {e}")
        else:
            print(f"✗ Test file not found: {test_file}")
    
    # Run the tests
    if suite.countTestCases() > 0:
        print(f"\nRunning {suite.countTestCases()} tests...")
        print("=" * 60)
        
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        
        print("=" * 60)
        print(f"Test Summary:")
        print(f"  Total Tests: {result.testsRun}")
        print(f"  Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
        print(f"  Failures: {len(result.failures)}")
        print(f"  Errors: {len(result.errors)}")
        
        if result.failures:
            print(f"\nFailures:")
            for test, traceback in result.failures:
                print(f"  - {test}: {traceback}")
        
        if result.errors:
            print(f"\nErrors:")
            for test, traceback in result.errors:
                print(f"  - {test}: {traceback}")
        
        # Return appropriate exit code
        return 0 if result.wasSuccessful() else 1
    else:
        print("No tests found to run!")
        return 1


def run_specific_test(test_file):
    """Run tests from a specific file."""
    if os.path.exists(test_file):
        print(f"Running tests from {test_file}...")
        
        # Import the test module dynamically
        module_name = test_file.replace('.py', '')
        try:
            # Add current directory to Python path
            sys.path.insert(0, '.')
            
            # Import the module
            module = __import__(module_name)
            
            # Load tests
            loader = unittest.TestLoader()
            suite = loader.loadTestsFromModule(module)
            
            # Run tests
            runner = unittest.TextTestRunner(verbosity=2)
            result = runner.run(suite)
            
            return 0 if result.wasSuccessful() else 1
            
        except ImportError as e:
            print(f"Failed to import {module_name}: {e}")
            return 1
        except Exception as e:
            print(f"Error running tests: {e}")
            return 1
    else:
        print(f"Test file not found: {test_file}")
        return 1


if __name__ == '__main__':
    # Check if specific test file was requested
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
        if test_file.endswith('.py') and test_file.startswith('test_'):
            exit_code = run_specific_test(test_file)
            sys.exit(exit_code)
        else:
            print(f"Invalid test file: {test_file}")
            print("Usage: python run_tests.py [test_file.py]")
            sys.exit(1)
    else:
        # Run all tests
        exit_code = discover_and_run_tests()
        sys.exit(exit_code)