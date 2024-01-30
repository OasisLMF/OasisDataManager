import pytest

from oasis_data_manager.df_reader.config import ConfigError, OasisReader, get_df_reader
from oasis_data_manager.df_reader.reader import OasisPandasReader


class DfReader(OasisReader):
    def __init__(self, *args, extra=None, **kwargs):
        self.extra = extra
        super().__init__(*args, **kwargs)


class NotDfReader:
    pass


config = {
    "default": {
        "path": "oasis_data_manager.df_reader.reader.OasisPandasReader",
        "options": {},
    },
    "test": {
        "path": "tests.df_reader.test_config.DfReader",
        "options": {
            "extra": "test value",
        },
    },
}


def test_file_path_is_given___default_class_is_used():
    reader = get_df_reader("testpath")

    assert type(reader) is OasisPandasReader


def test_non_path_or_dict_is_provided___error_is_raised():
    with pytest.raises(ConfigError) as exec_info:
        get_df_reader(1)

    assert str(exec_info.value) == "df_reader config must be a string or dictionary: 1"


def test_dict_without_path_is_provided___error_is_raised():
    with pytest.raises(ConfigError) as exec_info:
        get_df_reader({})

    expected = "df_reader config must provide a 'filepath' property: {}"
    actual = str(exec_info.value)

    assert actual == expected


def test_no_engine_config_is_provided___default_is_used():
    reader = get_df_reader({"filepath": "testpath"})

    assert type(reader) is OasisPandasReader


def test_engine_config_is_string___object_created_without_options():
    reader = get_df_reader(
        {
            "filepath": "testpath",
            "engine": "tests.df_reader.test_config.DfReader",
        }
    )

    assert type(reader) is DfReader
    assert reader.extra is None


def test_engine_config_is_provided_without_options___object_created_without_options():
    reader = get_df_reader(
        {
            "filepath": "testpath",
            "engine": {
                "path": "tests.df_reader.test_config.DfReader",
            },
        }
    )

    assert type(reader) is DfReader
    assert reader.extra is None


def test_engine_config_is_provided_with_options___object_created_with_options():
    reader = get_df_reader(
        {
            "filepath": "testpath",
            "engine": {
                "path": "tests.df_reader.test_config.DfReader",
                "options": {"extra": "test"},
            },
        }
    )

    assert type(reader) is DfReader
    assert reader.extra == "test"


def test_path_is_not_absolute_path___error_is_raised():
    with pytest.raises(ConfigError) as exec_info:
        get_df_reader(
            {
                "filepath": "test",
                "engine": {
                    "path": "path_to_a_module",
                    "options": {
                        "extra": "test value",
                    },
                },
            }
        )

    assert str(exec_info.value) == (
        "'path' found in the df_reader config is not valid: path_to_a_module"
    )


def test_module_doesnt_exist___error_is_raised():
    with pytest.raises(ImportError):
        get_df_reader(
            {
                "filepath": "test",
                "engine": {
                    "path": "tests.df_reader.test_config_other.DfReader",
                    "options": {
                        "extra": "test value",
                    },
                },
            }
        )


def test_class_doesnt_exist_in_module___error_is_raised():
    with pytest.raises(AttributeError):
        get_df_reader(
            {
                "filepath": "test",
                "engine": {
                    "path": "tests.df_reader.test_config.OtherDfReader",
                    "options": {
                        "extra": "test value",
                    },
                },
            }
        )


def test_reader_isnt_an_instance_of_the_base_reader____error_is_raised():
    with pytest.raises(ConfigError) as exec_info:
        get_df_reader(
            {
                "filepath": "test",
                "engine": {
                    "path": "tests.df_reader.test_config.NotDfReader",
                    "options": {
                        "extra": "test value",
                    },
                },
            }
        )

    assert str(exec_info.value) == "'NotDfReader' does not extend 'OasisReader'"
