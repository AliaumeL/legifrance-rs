# Dila Open Data 

**Warning**: this repository allows you to download and use datasets provided by
the [Dila][dila] (Direction de l'information légale et administrative) in a
simple way. However, the datasets have terms of use that you must respect.
Please refer to the [Dila website][donnees-juridiques] for more information on
the terms of use of the datasets. 

## Usage

This repository is a wrapper around two very different ways to interact with
the Dila datasets. On the one hand, it allows to download and index the
datasets yourself. On the other hand, it allows to use the API provided by the
Dila to access the datasets. If you are interested in a few results, then the
API is the best way to go. If you are interested in a lot of results, then it
is better to download the datasets yourself.

### Use the datasets

To download and index the datasets yourself use the following command:

```bash
dilarxiv --tarballs 
```

This will download *all* the datasets provided by the Dila. If you are
interested in a specific dataset, you can use the `--fond` option (any
number of times) to specify the datasets you are interested in. For example, to
download the *CASS* dataset, you can use the following command:

```bash
dilarxiv --tarballs --fond CASS
```

Note that datasets are available on the [open data portal][dila-opendata] of
the Dila. Therefore, it is possible to only download specific archives
and not whole datasets.

To automatically extract the datasets, you can use the `--extract` option. This
assumes that there is a `tarball` folder available, for instance because you
have just downloaded the datasets using the `--tarballs` option. 

```bash
dilarxiv --extract
```

Now, the extracted content is available in the `extracted` folder. The content
is organized in many subfolders, ultimately containing XML files.
To index the datasets, you can use the `--index` option. This will create a
`index` folder with the internal structure of the index allowing for fast
searches.

```bash
dilarxiv --index
```

**Warning:** indexing can be quite time / cpu consuming.

Now, to search for documents in the index, you can use the `--query` option. This
will perform a fulltext search and return the actual paths of
the files of interest.

```bash
dilarxiv --query "search term"
```

By default, the answer is just a list of ten results. If you want to built an
actual list of all the results, you can use the `--save` option that will
create a text file with one line per result.


### Use the API

To use the API, you need to create an account on
the [PISTE website][piste-api] that hosts the APIs. Following the
instructions, you will get an API key that must be stored in 
a file called `client-secret.txt` together with an identifier
that should be stored in a file called `client-id.txt`. 

```bash
dilapi --query "search term" --limit 1000
```

Note that by default, one gets exactly the results as answered by the API in
the JSON format. If you want to get the results in a more human readable
format, you can use the `--human` option. To fetch the actual content of the
results, and store them you can use the option `--store`. This will create a
folder `dilapi-store` with the content of the results. Beware that this can be
especially slow if a large number of results are needed.

## How to install

The easiest way to install the software is to download
one of the prebuilt binaries from github. If this is not
possible, rebuilding the software should be easy:

1. Clone the repository
2. Run `cargo build --release` to build the software
3. Run `cargo install --path .` to install the software

A relatively recent version of Rust is required to build the software.

## Status

- [x] Download datasets
- [x] Extract datasets
- [x] Index datasets
- [x] Search in index
- [ ] Store results in a file
- [x] Query using the API
- [x] Store API results in (several) files
- [ ] Command line interface for the API
- [ ] Display 10 top results in the command line
- [ ] Fetch full contents using the API
- [ ] Parse API results into a human readable format

## Notes

This repository is a proof of concept and will never be maintained or put into
production. The coding style is terrible, there is no documentation, no tests,
and minimal error handling. Please do not use this unless you understand what
you are doing.

[dila]: https://www.dila.premier-ministre.gouv.fr/
[donnees-juridiques]: https://www.dila.premier-ministre.gouv.fr/services/repertoire-des-informations-publiques/les-donnees-juridiques
[dila-opendata]: https://echanges.dila.gouv.fr/OPENDATA/
[piste-api]: https://piste.gouv.fr/
