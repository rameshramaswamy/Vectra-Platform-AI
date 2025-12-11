FROM python:3.11-slim
RUN pip install mlflow==2.6.0
EXPOSE 5000
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]