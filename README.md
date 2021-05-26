# Data Sync from S3 to Dropbox (for Daniel Chen)

## Running on server

```bash
# setup docker & git
sudo yum update -y
sudo amazon-linux-extras install docker -y
sudo service docker start
sudo usermod -a -G docker ec2-user
sudo yum install git -y

# get code & build image
git clone https://github.com/weilu/data-sync.git
docker build -t data-sync .

# Use -e MAX_FILE_SIZE_GB to process only files smaller than the specified size – useful when disk space is limited
docker run -e AWS_ACCESS_KEY_ID=[] -e AWS_SECRET_ACCESS_KEY=[] DROPBOX_TOKEN=[] data-sync:latest
```
