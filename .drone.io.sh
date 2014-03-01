#!/bin/sh
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/jre

sudo apt-get update
sudo apt-get install python2.6 python2.6-dev


##################
# Cassandra 1.1.x
##################
cd /usr/local
sudo curl -LO http://archive.apache.org/dist/cassandra/1.1.12/apache-cassandra-1.1.12-bin.tar.gz
sudo tar xf apache-cassandra-1.1.12-bin.tar.gz

# Increse stack-size: http://stackoverflow.com/questions/11901421/cannot-start-cassandra-db-using-bin-cassandra
sudo sed -ie 's/-Xss180k/-Xss280k/' /usr/local/apache-cassandra-1.1.12/conf/cassandra-env.sh

cd /home/ubuntu/src/bitbucket.org/tk0miya/testing.cassandra
pip install --use-mirrors --upgrade detox misspellings
find src/ -name "*.py" | misspellings -f -
detox
rm -rf /usr/local/apache-cassandra-1.1.12


##################
# Cassandra 1.2.x
##################
cd /usr/local
sudo curl -LO http://archive.apache.org/dist/cassandra/1.2.15/apache-cassandra-1.2.15-bin.tar.gz
sudo tar xf apache-cassandra-1.2.15-bin.tar.gz

cd /home/ubuntu/src/bitbucket.org/tk0miya/testing.cassandra
detox
rm -rf /usr/local/apache-cassandra-1.2.15


##################
# Cassandra 2.0.x
##################
cd /usr/local
sudo curl -LO http://archive.apache.org/dist/cassandra/2.0.5/apache-cassandra-2.0.5-bin.tar.gz
sudo tar xf apache-cassandra-2.0.5-bin.tar.gz

cd /home/ubuntu/src/bitbucket.org/tk0miya/testing.cassandra
detox
rm -rf /usr/local/apache-cassandra-2.0.5


##################
# Cassandra 2.1.x
##################
cd /usr/local
sudo curl -LO http://archive.apache.org/dist/cassandra/2.1.0/apache-cassandra-2.1.0-bin.tar.gz
sudo tar xf apache-cassandra-2.1.0-bin.tar.gz

cd /home/ubuntu/src/bitbucket.org/tk0miya/testing.cassandra
detox
rm -rf /usr/local/apache-cassandra-2.1.0
