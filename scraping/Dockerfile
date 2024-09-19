# Use the Ubuntu 22.04 base image
FROM ubuntu:22.04

# Add Python 3.8 to the image
FROM python:3.11

# Update package lists for the Ubuntu system
RUN apt-get update

# Install the 'unzip' package
RUN apt install unzip

# Copy the Chrome Debian package to the image
COPY chrome_114_amd64.deb ./

# Install the Chrome Debian package
RUN apt install ./chrome_114_amd64.deb -y

# Download ChromeDriver binary version 114.0.5735.90 for Linux
RUN wget https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip

# Unzip the downloaded ChromeDriver binary
RUN unzip chromedriver_linux64.zip

# Move the ChromeDriver binary to /usr/bin
RUN mv chromedriver /usr/bin/chromedriver

# Print the version of Google Chrome installed
RUN google-chrome --version

# Set the working directory inside the image to /app
WORKDIR /app

# install packages
RUN pip install selenium==4.0.0 supabase==2.7.4 cloudscraper==1.2.71 bs4==0.0.2

# Copy the Python script 'app.py' to /app
COPY product_price.py /app/
COPY util.py /app/

# Specify the default command to execute when the container starts
ENTRYPOINT [ "python", "product_price.py"]