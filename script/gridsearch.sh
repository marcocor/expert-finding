ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=$ROOT_DIR/../src/main/python
source $ROOT_DIR/../expert-finding.conf

python $ROOT_DIR/../src/main/python/gridsearch.py \
	-d $DATABASE_NAME \
	-c $CACHE_DIR \
	-l $LUCENE_INDEX_DIR \
	-w $WIKI_API_ENDPOINT \
    -r $RELATEDNESS_DICT \
	-g $TAGME_API_KEY \
    -t $ROOT_DIR/../datasets/tu-expert-collection-translated/tu-translated-queries.tsv \
	-q $ROOT_DIR/../corrected_GT5_judged_system_generated_graded.qrel \
	$@
