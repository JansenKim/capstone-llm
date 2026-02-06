FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.5.4-hadoop-3.3.6-v1

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir

COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY pyproject.toml ./
COPY src ./
COPY README.md ./
RUN pip install .

CMD ["python3","-m", "capstonellm.tasks.clean"]

#TODO add your project code and dependencies to the image

