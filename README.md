MongoTS
======

A fast API for time series storage in MongoDb

## Usage

```python
import mongots
client = mongots.MongoTSClient()
```

## Run tests

Integration test requires a MongoDb to be up (run docker-compose up).

Launch all tests:

```bash
pytest
```

Launch only unit test:

```bash
python -m unittest -v test
```
