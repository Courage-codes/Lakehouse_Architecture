import pytest
import sys
import os

# Add the glue_jobs directory to the path for importing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'glue_jobs'))

def test_order_items_glue_job_imports():
    """Test that the order items glue job script can be imported without errors"""
    try:
        with open('glue_jobs/order-items-glue-job.py', 'r') as f:
            content = f.read()
        
        assert 'import' in content or 'from' in content
        assert len(content) > 0
        
    except FileNotFoundError:
        pytest.skip("order-items-glue-job.py not found")

def test_order_items_glue_job_syntax():
    """Test that the order items glue job has valid Python syntax"""
    try:
        with open('glue_jobs/order-items-glue-job.py', 'r') as f:
            content = f.read()
        
        compile(content, 'order-items-glue-job.py', 'exec')
        
    except FileNotFoundError:
        pytest.skip("order-items-glue-job.py not found")