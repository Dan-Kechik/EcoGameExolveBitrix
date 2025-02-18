# Use the official Python image from the Docker Hub
FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages
RUN pip install flask
RUN pip install requests
RUN pip install pandas
RUN pip install fast_bitrix24

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Define environment variable
ENV FLASK_APP=flask_server.py

# Run the application
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]
