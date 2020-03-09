import argparse
from pathlib import Path
from processing.metrics import NGramPerplexityScorer


def main(dataset_path: Path, model_path: Path, order: int):
    with dataset_path.open('r') as inp:
        sents = inp.readlines()
    scorer = NGramPerplexityScorer()
    scorer.fit(sents, order)
    scorer.save(model_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset-path', type=str)
    parser.add_argument('--order', type=int, default=5)
    parser.add_argument('--model-path', type=str, default='resources/ppl_scorers/ngram_model.pkl')
    args = parser.parse_args()
    main(Path(args.dataset_path), Path(args.model_path), args.order)
