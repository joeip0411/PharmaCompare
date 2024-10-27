FROM python:3.11

# Update package lists
RUN apt-get update

# Install the 'unzip' package
RUN apt install unzip

# Copy the Chrome Debian package to the image
# Download from https://bestim.org/chrome-114.html
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

# install required packages
RUN pip install selenium==4.0.0 supabase==2.7.4 cloudscraper==1.2.71 bs4==0.0.2 boto3 pandas

COPY *.py /app/

# Specify the default command to execute when the container starts
ENTRYPOINT ["python", "-u"]