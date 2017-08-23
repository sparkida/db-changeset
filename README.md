DB Changesets [![Build Status][travis-badge]][travis-link] 
====================

NodeJS module for running changesets against databases


### Changesets

- Each changeset file should follow the naming convention `<FileId>.<SchemaVersion>.<Extension>`
- FileId is always unique and generally an increment from last id

### Examples

- **[Changesets](https://github.com/vertebrae-org/db-changeset/tree/master/examples)**
- **[Configuration](https://github.com/vertebrae-org/db-changeset/blob/master/config.sample.js)**

### How To Run

#### From Repo

```bash
node . <options> config.js
#or
./bin/changeset <options> config.js
```

#### As dependency (from npm)

```bash
changeset <options> config.js
```

[travis-badge]: https://travis-ci.org/vertebrae-org/db-changeset.svg?branch=master
[travis-link]: https://travis-ci.org/vertebrae-org/db-changeset
