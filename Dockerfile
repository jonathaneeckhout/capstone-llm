FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.5.4-hadoop-3.3.6-v1

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir

# Copy project code and dependencies
COPY src/ /opt/spark/work-dir/src/
COPY requirements.txt /opt/spark/work-dir/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Add the work directory to PYTHONPATH so Python can find capstonellm module
ENV PYTHONPATH=/opt/spark/work-dir/src

# Set the default command to run clean.py with default arguments
CMD ["python3", "src/capstonellm/tasks/clean.py", "--env", "local", "--tag", "python-polars"]
# CMD ["sh", "-c", "while true; do sleep 1; done"]