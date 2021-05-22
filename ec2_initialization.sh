#!/bin/bash
sudo apt update
sudo apt install python3.8
sudo apt-get install python3-pip

sudo apt install unzip
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

git clone https://github.com/rajeevdixit19/Scaleable-Ml.git
cd Scaleable-Ml
mkdir data

pip3 install -r requirements.txt
python3 feature_prep.py