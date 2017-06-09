ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=$ROOT_DIR/../src/main/python
source $ROOT_DIR/../expert-finding.conf

python $ROOT_DIR/../src/main/python/benchmark.py \
	-s $STORAGE_DIR \
	-l $LUCENE_INDEX_DIR \
	-d $DATABASE_NAME \
    -r $RELATEDNESS_DICT \
	-g $TAGME_API_KEY \
    -t $ROOT_DIR/../datasets/tu-expert-collection-translated/tu-translated-queries.tsv \
    -q $ROOT_DIR/../datasets/tu-expert-collection-translated/qrels/expert_finding/GT5_judged_system_generated_graded.qrel \
	-f eciaf_score efiaf_score
