install: requirements.txt 
	pip install --upgrade pip &&\
	pip install -r requirements.txt

setup: 
	# python setup.py install
	pip install . 

lint:
	pylint --disable=R,C *.py &&\
	pylint --disable=R,C metadata/*.py &&\
	pylint --disable=R,C metadata/tests/*.py

test:
	python -m pytest -vv --cov=metadata metadata/tests

format:
	black *.py &&\
	black metadata/*.py
	black metadata/tests/*.py

all: install setup lint format test 
