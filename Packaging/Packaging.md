# Initial Prepration
Install wheel for package distribution: ```python3 -m pip install --user --upgrade setuptools wheel```
Install twine: ```python3 -m pip install --user --upgrade twine```

# Build
The following will build the project under a new folder called dist: ```python3 setup.py sdist bdist_wheel```

# Upload to PYPI
```python3 -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*```
Verify it uploaded correctly: ```https://test.pypi.org/project/pysynctk```

# Install into new Project
```python3 -m pip install --index-url https://test.pypi.org/simple/ pysynctk```

# Add Dependency to Project
Create an entry in setup.py under *install_requires*:
```python
    install_requires=[
        'wheel',
        'pythonnet'
    ]
```
Test with the following command: `python setup.py develop`