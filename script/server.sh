ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=$ROOT_DIR/../src/main/python
source $ROOT_DIR/../expert-finding.conf

python $ROOT_DIR/../src/main/python/expertfinding/web/server.py \
	-s $STORAGE_DIR \
	-d $DATABASE_NAME \
	-g $TAGME_API_KEY \
	-r $RELATEDNESS_DICT \
	-l $LUCENE_INDEX_DIR