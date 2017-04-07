# expert-finding

Find experts, semantically.

## Directory structure

- `src/main/python`: All Python code
  - `./expertfinding`: Main EF module providing classes and methods to build and query the EF database.
  - `./expertfinding/preprocessing`: Executable code for EF database generation.
  - `./expertfinding/preprocessing/datasets_util`: Executable code for pre-processing raw datasets.
  - `./web`: Flask web server (offers APIs to query EF database).

## Pipeline
The pipeline is `Raw dataset` -> `EF database` -> `perform queries`

You may have to add the EF code to the python path. You can do so with:

```export PYTHON_PATH=$PYTHON_PATH:/path/to/src/main/python```

### Expert Finding Database creation
Creating the EF database from the raw dataset requires each document in the raw dataset to be annotated by TagMe. You will have to [register to Tagme](https://sobigdata.d4science.org/group/tagme/) and get an authentication token. Create the database by running a command like (for the TU dataset):

```
python expertfinding/preprocessing/create_db.py                   \
    -f tu                                                         \
    -i /path/to/datasets/tu-expert-collection-translated/full.csv \
    -c /path/to/storage/cache                                     \
    -s /path/to/storage/tu.db                                     \
    -g <gcube-token>
```

For more information on the command options, run `create_db.py -h`.

The EF database will appear in `/path/to/storage/tu.db`

### Web Server
The EF dataset can be queried though a RESTful API provided by a Flask server. You can launch the server with:

```
python expertfinding/web/server.py  \
    -r /path/to/storage/relatedness \
    -s /path/to/storage/tu.db       \
    -g <gcube-token>
```
The web server is accessible at `http://localhost:5000`. APIs are accessible E.g. at `http://localhost:5000/query?q=data+structures`.

### Benchmark
To launch the benckmark, you need to have a set of queries (topics to search) and the associated ground truth (ordered list of experts that match the query in a Qrel file). Moreover, you will need the [official trec_eval](http://trec.nist.gov/trec_eval/) binary installed in your system (i.e. you have to be able to run `trec_eval` from command line).

The benchmark can be run as:
```
python benchmark.py -s /path/to/storage/tu.db                                      \
    -r /path/to/storage/relatedness                                                \
    -f eciaf_score efiaf_score                                                     \
    -g <gcube-token>                                                               \
    -t /path/to/datasets/tu-expert-collection-translated/tu-translated-queries.tsv \
    -q /path/to/datasets/tu-expert-collection-translated/qrels/expert_finding/GT1_self_selected_all_experts.qrel
```

The script will generate an output for each tested scoring function (chosen with option `-f`, E.g. in the example we have tested two scoring functions). The query file is specified with the `-t` parameter, while the ground truth qrel file is specified with the `-q` parameter. For more information on the command options, run `create_db.py -h`.

