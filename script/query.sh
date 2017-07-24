ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=$ROOT_DIR/../src/main/python
source $ROOT_DIR/../expert-finding.conf

python $ROOT_DIR/../src/main/python/query.py \
	-d $DATABASE_NAME \
	-g $TAGME_API_KEY \
	-c $CACHE_DIR \
	-l $LUCENE_INDEX_DIR \
	-w $WIKI_API_ENDPOINT