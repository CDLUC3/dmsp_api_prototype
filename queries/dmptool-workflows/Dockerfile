FROM quay.io/astronomer/astro-runtime:9.20.0

# Root user for installations
USER root

# Install git
RUN apt-get update && apt-get install git -y
USER astro

# Install Observatory Platform
RUN git clone --branch feature/astro_kubernetes https://github.com/The-Academic-Observatory/observatory-platform.git
RUN pip install -e ./observatory-platform/[tests] --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-no-providers-3.10.txt
