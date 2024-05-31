from pathlib import Path
from sync import sync


def test_when_a_file_exists_in_the_source_but_not_the_destination():
    fakefs = FakeFilesystem({
        '/src': {"hash1": "fn1"},
        '/dst': {},
    })
    sync('/src', '/dst', filesystem=fakefs)
    assert fakefs.actions == [("COPY", Path("/src/fn1"), Path("/dst/fn1"))]


def test_when_a_file_has_been_renamed_in_the_source():
    fakefs = FakeFilesystem({
        '/src': {"hash1": "fn1"},
        '/dst': {"hash1": "fn2"},
    })
    sync('/src', '/dst', filesystem=fakefs)
    assert fakefs.actions == [("MOVE", Path("/dst/fn2"), Path("/dst/fn1"))]


class FakeFilesystem:
    def __init__(self, path_hashes):
        self.path_hashes = path_hashes
        self.actions = []

    def read(self, path):
        return self.path_hashes[path]

    def copy(self, source, dest):
        self.actions.append(('COPY', source, dest))

    def move(self, source, dest):
        self.actions.append(('MOVE', source, dest))

    def delete(self, dest):
        self.actions.append(('DELETE', dest))
