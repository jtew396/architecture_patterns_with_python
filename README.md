# Application code for Architecture Patterns with Python by Bob Gregory and Harry Percival


## Requirements

```sh
python3.8 -m venv virtualenv
source virtualenv/bin/activate

# For Chapter 1
pip install pytest

# For Chapter 2
pip install pytest sqlalchemy

# For Chapter 3
pip install pytest sqlalchemy

# For Chapter 4
pip install -r requirements.txt
```

## Running the tests

```sh
# For Chapters 1-3
pytest test_example.py

# For Chapter 4
docker compose up
pytest
```