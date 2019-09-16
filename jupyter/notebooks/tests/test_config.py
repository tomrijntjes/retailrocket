import pytest

from src.config import read_table

def test_read_table():
    category_tree = read_table("category_tree")
    assert category_tree.count() == 1669
    events = read_table("events")
    assert events.count() == 2756101

@pytest.mark.skip(reason="Causes a heap space error in the current setup")
def test_read_table_extensive():
    item_properties = read_table("item_properties")
    assert item_properties.count() == 20262700
