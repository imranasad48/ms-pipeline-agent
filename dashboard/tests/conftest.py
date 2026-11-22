import pytest

from healing.tests.conftest import Toy  # noqa: F401  (toy repo + incidents DB, no LLM)


@pytest.fixture
def toy(tmp_path) -> Toy:
    return Toy(tmp_path)
