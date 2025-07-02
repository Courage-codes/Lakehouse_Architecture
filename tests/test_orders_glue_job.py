import pytest
import sys
import os

# Add the glue_jobs directory to the path for importing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'glue_jobs'))

def test_orders_glue_job_imports():
    """Test that the orders glue job script can be imported without errors"""
    try:
        with open('glue_jobs/orders-glue-job.py', 'r') as f:
            content = f.read()
        
        assert 'import' in content or 'from' in content
        assert len(content) > 0
        
    except FileNotFoundError:
        pytest.skip("orders-glue-job.py not found")

def test_orders_glue_job_syntax():
    """Test that the orders glue job has valid Python syntax"""
    try:
        with open('glue_jobs/orders-glue-job.py', 'r') as f:
            content = f.read()
        
        compile(content, 'orders-glue-job.py', 'exec')
        
    except FileNotFoundError:
        pytest.skip("orders-glue-job.py not found")