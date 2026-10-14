import os

# Rich's shared console keeps a COLUMNS set when it's created, at import, ahead of any COLUMNS a test sets later. Tests
# that need a width set COLUMNS themselves, so drop an inherited one before anything imports discogskit.
os.environ.pop("COLUMNS", None)
