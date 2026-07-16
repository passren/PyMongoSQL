# -*- coding: utf-8 -*-
"""Commands must decode with the database's codec options, not DEFAULT_CODEC_OPTIONS."""
import uuid
from unittest.mock import MagicMock

from bson.binary import Binary, UuidRepresentation
from bson.codec_options import CodecOptions

from pymongosql.executor import _run_db_command
from pymongosql.result_set import ResultSet
from pymongosql.sql.query_builder import QueryExecutionPlan

STANDARD_OPTS = CodecOptions(uuid_representation=UuidRepresentation.STANDARD)


def make_db(codec_options=STANDARD_OPTS, command_result=None):
    db = MagicMock()
    db.codec_options = codec_options
    db.command.return_value = command_result if command_result is not None else {"ok": 1}
    return db


def test_run_db_command_passes_codec_options():
    db = make_db()
    connection = MagicMock(retry_config=None, session=None)

    _run_db_command(db, {"find": "users"}, connection, "find")

    assert db.command.call_args.kwargs["codec_options"] is STANDARD_OPTS


def test_run_db_command_passes_codec_options_in_transaction():
    db = make_db()
    connection = MagicMock(retry_config=None)
    connection.session.in_transaction = True

    _run_db_command(db, {"find": "users"}, connection, "find")

    kwargs = db.command.call_args.kwargs
    assert kwargs["codec_options"] is STANDARD_OPTS
    assert kwargs["session"] is connection.session


def test_getmore_passes_codec_options():
    """Ensure a cursor spanning batches decodes every batch the same way"""
    db = make_db(
        command_result={"cursor": {"id": 0, "nextBatch": [{"_id": 2}]}},
    )
    plan = QueryExecutionPlan(collection="users", projection_stage={"_id": 1})
    result_set = ResultSet(
        command_result={"cursor": {"id": 99, "firstBatch": [{"_id": 1}]}},
        execution_plan=plan,
        database=db,
    )

    result_set.fetchall()

    assert db.command.called, "getMore was never issued"
    assert db.command.call_args.kwargs["codec_options"] is STANDARD_OPTS


def test_standard_codec_options_decode_subtype_4_uuid():
    """Check subtype-4 UUIDs decode to uuid.UUID rather than staying Binary"""
    value = uuid.UUID("12345678-1234-5678-1234-567812345678")
    encoded = Binary.from_uuid(value, UuidRepresentation.STANDARD)

    assert isinstance(encoded.as_uuid(UuidRepresentation.STANDARD), uuid.UUID)
    assert STANDARD_OPTS.uuid_representation == UuidRepresentation.STANDARD
