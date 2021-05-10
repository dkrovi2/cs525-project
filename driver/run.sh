#!/bin/sh

python sim.py -d ../../data/phishing-dataset.txt -t test-topic-20210509-ring-4 -p 4  -s 10000 -e 10000 -g ring

python sim.py -d ../../data/phishing-dataset.txt -t test-topic-20210509-none-8 -p 8  -s 10000 -e 6250 
python sim.py -d ../../data/phishing-dataset.txt -t test-topic-20210509-fcg-8 -p 8  -s 10000 -e 6250 -g fcg
python sim.py -d ../../data/phishing-dataset.txt -t test-topic-20210509-ring-8 -p 8  -s 10000 -e 10000 -g ring

python sim.py -d ../../data/phishing-dataset.txt -t test-topic-20210509-none-10 -p 10  -s 10000 -e 5000 
python sim.py -d ../../data/phishing-dataset.txt -t test-topic-20210509-fcg-10 -p 10  -s 10000 -e 5000 -g fcg
python sim.py -d ../../data/phishing-dataset.txt -t test-topic-20210509-ring-10 -p 10  -s 10000 -e 5000 -g ring
