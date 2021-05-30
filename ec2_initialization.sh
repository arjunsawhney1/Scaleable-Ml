#!/bin/bash
sudo apt update
sudo apt-get install openjdk-8-jre
sudo apt install python3.8
sudo apt-get install python3-pip

sudo apt install unzip
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_SESSION_TOKEN=

git clone https://github.com/rajeevdixit19/Scaleable-Ml.git
cd Scaleable-Ml
mkdir data

pip3 install -r requirements.txt
pip3 install "dask[distributed]" --upgrade
pip3 install fastparquet

python3 feature_prep.py