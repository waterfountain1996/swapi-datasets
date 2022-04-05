import petl as etl
import requests


BASE_URL = "https://swapi.dev/api"

s = requests.Session()


def load_people_page(page: int = 1) -> tuple[list[dict], bool]:
    """Fetch people from SWApi.

    Args:
        page: Page to fetch.

    Returns:
        A tuple where the first element is a list of people, and the
            second is a bool indicating where there's a next page.
    """
    url = f"{BASE_URL}/people"
    resp = s.get(url, params={"page": page})
    data = resp.json()
    return data["results"], data["next"] is not None


def load_people():
    """SWApi people loader generator.

    Yields pages of people.
    """
    page = 1
    has_more = True
    while has_more:
        people, has_more = load_people_page(page)
        yield people
        page += 1


def load_planet(url: str, cache: dict[str, str]) -> str:
    """Get planet name from url.

    Provides a caching mechanism using a dictionary.

    Args:
        url: Planet endpoint url.
        cache: Cache dictionary.
    """
    if url in cache:
        return cache[url]

    resp = s.get(url)
    name = resp.json()["name"]
    return cache.setdefault(url, name)


def read_dataset(filename: str, nrows: int = 10, skip: int = 0) -> list[dict]:
    """Read `nrows` from dataset with optional offset `skip`.

    Returns:
        list of dictionaries.
    """
    table = etl.fromcsv(filename).rowslice(skip, skip + nrows)
    return list(table.dicts())


def read_and_aggregate_columns(
    filename: str, columns: list[str]
) -> list[dict]:
    """Read specified `columns` from dataset and include column displaying
    duplicate row count.

    Returns:
        list of dictionaries
    """
    table = (
        etl.fromcsv(filename).cut(*columns).distinct(key=columns, count="count")
    )
    return list(table.dicts())
