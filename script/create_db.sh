ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=$ROOT_DIR/../src/main/python
source $ROOT_DIR/../expert-finding.conf
echo $DATASET_PATH

python $ROOT_DIR/../src/main/python/expertfinding/preprocessing/create_db.py \
	-i "$DATASET_PATH" \
	-f $DATASET_TYPE \
	-s $STORAGE_DIR \
	-c $CACHE_DIR \
	-l $LUCENE_INDEX_DIR \
	-g $TAGME_API_KEY

