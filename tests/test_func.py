import pytest
from driverlicense.testing import AgofTesting

@pytest.fixture(scope="module")
def ag():
    return AgofTesting()


def test_size(ag):
    data = ag.get_data()
    assert 90 == len(data)


def test_df(ag):
    df = ag.process_data(n=10)
    columns = ['Analyse', 'Fälle (ungew) auf Basis ZG/Vorfilter', 'Grundgesamtheit',
    'Kontakte  Mio', 'Kontakte Mio', 'Medientyp', 'Studien- teil',
    'Studienteil', 'Titel', 'Ungew Fälle  auf Basis  ZG/Vorfilter',
    'Ungew Fälle auf Basis Gesamt 10+', 'Ungew Fälle auf Basis Gesamt 16+',
    'Ungew Fälle auf Basis ZG/Vorter', 'Unique User  %',
    'Unique User  Mio', 'Unique User %', 'Unique User Mio', 'Vorfilter',
    'Zeitraum', 'Zielgruppe']

    assert all([a == b for a, b in zip(columns, df.columns)])