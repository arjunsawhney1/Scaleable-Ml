#!/bin/bash
sudo apt update
apt install python3.8
python -m pip install pip

git clone https://github.com/rajeevdixit19/Scaleable-Ml.git
cd Scaleable-Ml

pip install -r requirements.txt
python feature_prep.py