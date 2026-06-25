FROM python:3.12-slim

WORKDIR /app
COPY dist/*.whl ./
RUN pip install $(ls -t *.whl | head -1) && rm *.whl

EXPOSE 8080

ENTRYPOINT ["vmm"]
