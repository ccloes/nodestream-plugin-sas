# nodestream-plugin-sas

Reads `.sas7bdat` files with a Nodestream extractor.

```yaml
- implementation: nodestream_plugin_sas:Sas7bdatExtractor
  arguments:
    globs:
      - data/*.sas7bdat
```

Install Nodestream, then this plugin:

```bash
pip install nodestream nodestream-plugin-sas
```

With Poetry:

```bash
poetry add nodestream nodestream-plugin-sas
```

Name `Sas7bdatExtractor` in the pipeline. `.SAS7BDAT` is read the same way as `.sas7bdat`. `.gz` and `.bz2` are decompressed first. A `.zip` yields each sas7bdat member and skips the other members. `chunksize` defaults to 10000.

Each row is a dictionary keyed by column name. SAS numbers stay floats, including whole numbers such as `2.0`. Missing values are `None`. SAS dates and datetimes are Python `datetime` objects. Text uses the encoding declared in the file. A byte that does not fit that encoding is replaced, and the rest of the file is still read.

## Tests

```bash
poetry install
poetry run pytest
```
