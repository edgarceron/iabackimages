#Repo to upload label images to S3

docker run -e LABEL_FILE=YOUR_LABEL_FILE -e AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY_ID -e AWS_BUCKET_NAME=YOUR_BUCKET_NAME edgarceron1993/iabackimages