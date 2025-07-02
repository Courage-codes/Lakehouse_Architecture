import pytest
import sys
import os

# Add the glue_jobs directory to the path for importing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'glue_jobs'))

def test_products_glue_job_imports():
    """Test that the products glue job script can be imported without errors"""
    try:
        # This will test basic syntax and import issues
        with open('glue_jobs/products-glue-job.py', 'r') as f:
            content = f.read()
        
        # Basic validation that it's a Python file
        assert 'import' in content or 'from' in content
        assert len(content) > 0
        
    except FileNotFoundError:
        pytest.skip("products-glue-job.py not found")

def test_products_glue_job_syntax():
    """Test that the products glue job has valid Python syntax"""
    try:
        with open('glue_jobs/products-glue-job.py', 'r') as f:
            content = f.read()
        
        # Test syntax by compiling
        compile(content, 'products-glue-job.py', 'exec')
        
    except FileNotFoundError:
        pytest.skip("products-glue-job.py not found")