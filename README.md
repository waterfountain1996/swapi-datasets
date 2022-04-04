# SWApi dataset loader

## Setup:

- Install packages:

```bash
virtualenv venv
source ./venv/bin/activate
pip install -r ./requirements.txt
```

- Run migrations:

```bash
python ./manage.py makemigrations
python ./manage.py migrate
```

- Run RabbitMQ on port 5672 and start celery:

```bash
celery -A swapi:celery_app worker -l INFO
```

- Run development server:

```bash
python ./manage.py runserver
```

API has the following endpoints:

- /api/fetch - Load dataset in the background.
- /api/datasets - List of available datasets.
- /api/datasets/<id> - Dataset JSON endpoint.
- /api/datasets/<id>/columns - Column aggregation.
