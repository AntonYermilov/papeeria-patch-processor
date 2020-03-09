import sys
from pathlib import Path
from typing import List
import pickle

from nltk.lm.models import Laplace
from nltk.lm.preprocessing import padded_everygram_pipeline, pad_both_ends
from nltk.util import ngrams


class NGramPerplexityScorer:
    def __init__(self):
        self.model = None
        self.order = None

    def fit(self, text: List[str], order: int):
        self.model = Laplace(order)
        self.order = order
        train_data, padded_sents = padded_everygram_pipeline(order, text)

        print('Fitting n-gram model', file=sys.stderr)
        self.model.fit(train_data, padded_sents)
        print(f'Vocabulary size: {self.model.vocab}', file=sys.stderr)

    def save(self, path: Path):
        if self.model:
            model = {
                'model': self.model,
                'order': self.order
            }
            if not path.parent.exists():
                path.parent.mkdir(parents=True)
            with path.open('wb') as model_path:
                pickle.dump(model, model_path)

    def load(self, path: Path):
        with path.open('rb') as model_path:
            model = pickle.load(model_path)
            self.model = model['model']
            self.order = model['order']

    def perplexity(self, sent: str):
        text = pad_both_ends(sent, n=self.order)
        text_ngrams = ngrams(text, n=self.order)
        return self.model.perplexity(text_ngrams)
