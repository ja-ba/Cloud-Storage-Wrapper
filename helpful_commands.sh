#Setting up and accessing virtual environment
python3.10 -m venv .venv
source .venv/Scripts/activate

#Installing build dependencies
python -m pip install --upgrade pip
python -m pip install build
python -m pip install pre-commit
pre-commit install

#Reactivate venv
deactivate && source .venv/Scripts/activate

# installing dependencies
pip install -e '.[test]'


#Building the package
pyproject-build

#Running tox in parallel:
tox -p

#Running specifix tox env:
tox -e python310

#Running tox where env is recreated to install new dependencies:
tox --recreate

# Run and build
tox | tox -e ruff, typecheck, format, build

# Run pytest:
python310 -m pytest -s --cov


git push origin --tags
