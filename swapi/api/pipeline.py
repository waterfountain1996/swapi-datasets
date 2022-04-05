import datetime as dt

from celery import shared_task
import petl as etl

from .fetch import load_people, load_planet
from .models import Dataset

from ..settings import BASE_DIR


def process_people(
    people: list[dict],
    outfile: str,
    cache: dict[str, str],
    append: bool = False,
):
    """Process a list of person dictionaries.

    Args:
        people: List of people dictionaries.
        outfile: CSV file for processed data.
        cache: Cache dictionary for fetching planet names.
        append: Whether to write new file or append to it.

    """
    table = (
        etl.fromdicts(people)
        .convert("homeworld", lambda v: load_planet(v, cache))
        .addfield(
            "date",
            lambda r: (
                dt.datetime.fromisoformat(r["edited"].split(".")[0]).strftime(
                    "%Y-%m-%d"
                )
            ),
        )
        .cutout(
            [
                "url",
                "created",
                "edited",
                "films",
                "species",
                "vehicles",
                "starships",
            ]
        )
    )

    #
    write = etl.appendcsv if append else etl.tocsv
    return write(table, outfile)


@shared_task
def fetch_dataset() -> Dataset:
    """Fetch all people from SWApi and write to csv file.

    Returns newly fetched dataset.
    """
    # Prepare filename and a model instance for the dataset.
    now = dt.datetime.now()
    filename = str(BASE_DIR / f"{now.strftime('%Y-%m-%d %H:%M:%S')}.csv")
    dataset = Dataset(filename=filename, timestamp=now)

    cache = {}
    for i, people in enumerate(load_people()):
        # Process and save every batch. For each batch that isn't the
        # first one (i == 0) append to file insted of overwriting.
        process_people(people, filename, cache, append=i > 0)

    dataset.save()
    return dataset
