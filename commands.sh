# initialize
sudo yum update -y
sudo yum install docker
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo gpasswd -a $USER docker
newgrp docker
sudo yum install python-pip
sudo pip install docker-compose

# copy files to ec2
scp -r -i <key-pair>.pem <source-folder> \
ec2-user@<ec2-instance-name>.compute.amazonaws.com:/home/ec2-user/<destination-folder>

# start docker
sudo systemctl start docker
# Stop Docker
sudo systemctl stop docker

# docker-compose
docker-compose up

# Test data preparation
docker exec -i -t nifi bash
cd /opt/workspace/nifi/FakeDataset