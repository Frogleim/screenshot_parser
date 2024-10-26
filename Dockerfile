FROM python:3.9-slim
WORKDIR /app
RUN apt clean && apt update && apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev

RUN \
	apt-get install -y \
    nano \
	libgstreamer1.0-0 \
	gstreamer1.0-plugins-base \
	gstreamer1.0-plugins-good \
	gstreamer1.0-plugins-bad \
	gstreamer1.0-plugins-ugly \
	gstreamer1.0-libav \
	gstreamer1.0-tools \
	libgstreamer1.0-dev \
	libgstreamer-plugins-base1.0-dev


RUN apt-get update && apt-get install -y inotify-tools
RUN python -m pip install --upgrade pip
COPY . /app

RUN pip install --no-cache-dir --default-timeout=1000 -r requirements.txt
CMD ["python", "upload_video_consumer.py"]
