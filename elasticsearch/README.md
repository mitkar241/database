# Elasticsearch
---
This directory contains Elasticsearch Wrapper functions.

## Installation
---
- [install.sh](scripts/install.sh)

## Documentation
---
https://elasticsearch-py.readthedocs.io/en/v8.0.0/api.html#module-elasticsearch

## Known Issues
---
```
/usr/lib/python3/dist-packages/requests/__init__.py:89: RequestsDependencyWarning: urllib3 (1.26.8) or chardet (3.0.4) doesn't match a supported version!
  warnings.warn("urllib3 ({}) or chardet ({}) doesn't match a supported "
```

#### Solution
```bash
pip3 install --upgrade requests
```
