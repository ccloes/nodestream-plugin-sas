# nodestream-plugin-sas

Reads `.sas7bdat` files from Nodestream's `FileExtractor`.

```yaml
- implementation: nodestream.pipeline.extractors.files:FileExtractor
  arguments:
    globs:
      - data/*.sas7bdat
```

Install Nodestream, then this plugin:

```bash
pip install nodestream nodestream-plugin-sas
```

`FileExtractor` loads the `file_formats` entry point, so a `.sas7bdat` file is read without naming the plugin in the pipeline. `.SAS7BDAT` uses the same codec. `.gz`, `.bz2`, and `.zip` are handled by Nodestream before this codec sees the member.

Each row is a dictionary keyed by column name. SAS numbers stay floats, including whole numbers such as `2.0`. Missing values are `None`. SAS dates and datetimes are Python `datetime` objects. Text uses the encoding declared in the file. A byte that does not fit that encoding is replaced, and the rest of the file is still read.

This plugin needs a Nodestream build that imports `file_formats` plugins when a `FileExtractor` is created. That hook is on `cursor/file-format-plugins-f892` in [ccloes/nodestream](https://github.com/ccloes/nodestream) until it is released.

## Tests

```bash
pip install -e /path/to/nodestream
pip install -e . --no-deps
pip install pytest pytest-asyncio pytest-mock
pytest
```
