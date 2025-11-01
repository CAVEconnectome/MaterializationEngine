from functools import partial
from itertools import groupby, islice, repeat, takewhile
from operator import itemgetter

from sqlalchemy.exc import IntegrityError, SQLAlchemyError


def chunk_rows(data, chunksize: int = None):
    if chunksize:
        i = iter(data)
        generator = (list(islice(i, chunksize)) for _ in repeat(None))
    else:
        generator = iter([data])
    return takewhile(bool, generator)


def create_sql_rows(session, data_dict: dict, model):
    """Yields a dictionary if the record's id already exists, a row object
    otherwise.

    TODO: strip unneeded if else statements
    """
    ids = {item[0] for item in session.query(model.id)}
    for data in data_dict:
        is_row = hasattr(data, "to_dict")
        if is_row and data.get("id") in ids:
            yield data.to_dict(), True
        elif is_row:
            yield data, False
        elif data.get("id") in ids:
            yield data, True
        else:
            yield model(**data), False


def upsert(session, data, model, chunksize=None):

    for records in chunk_rows(data, chunksize):
        resources = create_sql_rows(session, records, model)
        sorted_resources = sorted(resources, key=itemgetter(1))
        for key, group in groupby(sorted_resources, itemgetter(1)):
            data = [g[0] for g in group]

            if key:
                session_upsert = partial(session.bulk_update_mappings, model)
            else:
                session_upsert = session.add_all
            try:
                session_upsert(data)
                session.commit()
            except IntegrityError:
                session.rollback()
                upsert(session, data, model)
            except Exception as e:
                session.rollback()
                num_rows = len(data)

                if num_rows > 1:
                    upsert(session, data, model, num_rows // 2)
                else:
                    raise SQLAlchemyError
