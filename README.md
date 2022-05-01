# Batch-Processing

Our machine has Redis, RabbitMQ and Celery installed

Redis : key-value store
RabbitMQ : message queuing
Celery : worker manager

We have implemented a word-count application for counting the words in the tweets of given dataset.
The data is stored in the Redis store.
We do batch processing, all the tweets in dataset are processed, then words are counted asyncronously.
