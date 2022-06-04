import sys, os

sys.path.append(os.path.abspath(os.path.join(__file__, "../..")))

from modules.data_gathering import get_data_sample
from pandas import DataFrame




def test_data_sample():
  a = get_data_sample(100_000, "flights_test")
  try: 
    assert isinstance(a, DataFrame)
    print("Passed")
    assert len(a) == 100_000
  except AssertionError:
    print("One of the tests has failed: ")

if __name__ == "__main__":
  test_data_sample()