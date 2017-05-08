ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=$ROOT_DIR/../src/main/python
source $ROOT_DIR/../expert-finding.conf

python $ROOT_DIR/../src/main/python/query.py \
	-s $STORAGE_DIR \
	-d $DATABASE_NAME \
	-g $TAGME_API_KEY \
	-l $LUCENE_INDEX_DIR
