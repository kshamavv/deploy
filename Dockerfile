# Use the official Python image as the base image (choose the appropriate version)
FROM python:3.8-slim

# Set the working directory within the container to /app
WORKDIR /app

# Copy the contents of the current directory to the container's /app directory
COPY . /app

# Install any necessary Python dependencies from a requirements file (if needed)
# COPY requirements.txt /app/requirements.txt
# RUN pip install -r /app/requirements.txt

# Define any environment variables if required
# ENV MY_ENV_VAR=value

# Expose any necessary ports if your application listens on a specific port
# EXPOSE 8080

# Specify the command to run your application
CMD ["python", "main.py"]

