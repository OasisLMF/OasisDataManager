import pytest
import respx
from httpx import ConnectError, Response

from oasis_data_manager.complex.complex import RestComplexData


class TestRestComplexData(RestComplexData):
    url = "https://test.com/xyz/"

    def get_headers(self):
        return {"a": "b", "c": "d"}


@pytest.fixture()
def mocked_test_api():
    with respx.mock(assert_all_called=False) as respx_mock:
        get_route = respx_mock.get(TestRestComplexData.url, name="fetch_get")
        get_route.return_value = Response(200, json={"echo": "test"})

        post_route = respx_mock.post(
            TestRestComplexData.url, name="fetch_post", json={"x": 1}
        )
        post_route.return_value = Response(200, json={"echo": "test"})

        yield respx_mock


def test_fetch__success(mocked_test_api):
    response = TestRestComplexData().fetch()

    assert response == {"echo": "test"}
    assert mocked_test_api["fetch_get"].called


def test_fetch__headers_set():
    assert TestRestComplexData().get_headers() == {"a": "b", "c": "d"}


def test_fetch__success__post(mocked_test_api):
    instance = TestRestComplexData()
    instance.get_post_json = lambda *a, **k: {"x": 1}

    response = instance.fetch()

    assert response == {"echo": "test"}
    assert mocked_test_api["fetch_post"].called


def test_fetch__handled_error(mocked_test_api, caplog):
    mocked_test_api["fetch_get"].return_value = Response(500)
    response = TestRestComplexData().fetch()

    assert not response
    assert mocked_test_api["fetch_get"].called
    assert caplog.records[0].message == "Exception in complex data call"
    assert caplog.records[0].exception == "Unexpected status in complex data call"


def test_fetch__unhandled_error(mocked_test_api, caplog):
    mocked_test_api["fetch_get"].side_effect = ConnectError
    TestRestComplexData().fetch()

    assert mocked_test_api["fetch_get"].called
    assert caplog.records[0].message == "Exception in complex data call"
    assert caplog.records[0].exception == "Mock Error"
