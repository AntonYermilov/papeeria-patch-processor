import sys
import psutil
import numpy as np
import pandas as pd
from dataclasses import dataclass
from argparse import ArgumentParser
from pathlib import Path
from cosmas.generated.cosmas_pb2 import PatchList
from processing.patch_processor import SimplePatchProcessor, AdvancedPatchProcessor
from processing.selector import select_sentence_pairs


def install_dependencies():
    import ssl
    try:
        _create_unverified_https_context = ssl._create_unverified_context
    except AttributeError:
        pass
    else:
        ssl._create_default_https_context = _create_unverified_https_context
    try:
        import nltk
        print('Installing nltk.punkt', file=sys.stderr)
        nltk.download('punkt', raise_on_error=True)
        print('Installing nltk.crubadan', file=sys.stderr)
        nltk.download('crubadan', raise_on_error=True)
    except:
        print('Unable to download some of dependencies, check your internet connection', file=sys.stderr)
        return False


@dataclass
class Parameters:
    min_length: int = 1
    max_length: int = None
    min_edit_distance: int = 1
    max_edit_distance: int = None
    min_alpha_ratio: float = 0.65

    def __init__(self, arguments):
        self.min_length = arguments.min_length
        self.max_length = arguments.max_length
        self.min_edit_distance = arguments.min_edit_dist
        self.max_edit_distance = arguments.max_edit_dist
        self.min_alpha_ratio = arguments.min_alpha_ratio


def main(dataset_path: Path, parameters: Parameters):
    content_dir = Path('resources', 'content')
    patches_dir = Path('resources', 'patches')

    num_cpus = psutil.cpu_count(logical=True)
    print(f'num_cpus={num_cpus}', file=sys.stderr)
    processor = AdvancedPatchProcessor(num_cpus=num_cpus)

    for doc_id in content_dir.iterdir():
        doc_id = doc_id.name

        doc_path = content_dir / doc_id
        patches_path = patches_dir / doc_id

        doc_iter = filter(lambda doc: doc.is_file(), doc_path.rglob('*'))
        doc_iter = sorted(doc_iter, key=lambda doc: int(doc.name))

        patches_iter = filter(lambda patch: patch.is_file(), patches_path.rglob('*'))
        patches_iter = sorted(patches_iter, key=lambda patch: int(patch.name))
        patches_iter = iter(patches_iter)

        for doc in doc_iter:
            content = doc.read_text()

            patches = []
            for patch in patches_iter:
                patch_list = PatchList()
                patch_list.ParseFromString(patch.read_bytes())
                patches.extend(patch_list.patches)
                if patch.name == doc.name:
                    break

            patches.sort(key=lambda p: p.timestamp)
            processor.process_patches(content, patches)

    sentence_pairs = list(processor.get_diffs())

    print(f'Selecting sentence pairs', file=sys.stderr)

    sentence_pairs = select_sentence_pairs(
        sentence_pairs,
        sent_regex=r'^[a-zA-Z][a-zA-Z@#№_();:\'"<>,.?!\s=*/+-]+[.?!;]$',
        min_length=parameters.min_length,
        max_length=parameters.max_length,
        min_char_levenshtein=parameters.min_edit_distance,
        max_char_levenshtein=parameters.max_edit_distance,
        min_alpha_ratio=parameters.min_alpha_ratio,
        perplexity_scorer=None
    )

    sentence_pairs = [(i, sp.source_sent, sp.target_sent) for i, sp in enumerate(sentence_pairs)]
    df = pd.DataFrame(
        data=np.array(sentence_pairs, dtype=np.object),
        index=None,
        columns=['sent_id', 'original_sent', 'edited_sent'],
    )
    df.to_csv(dataset_path, sep='\t', index=False)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--dataset', type=str,
                        help='File to save dataset')
    parser.add_argument('--min-length', type=int, default=1,
                        help='Minimal length of sentences')
    parser.add_argument('--max-length', type=int, default=None,
                        help='Maximal length of sentences')
    parser.add_argument('--min-edit-dist', type=int, default=1,
                        help='Minimal edit distance between sentences')
    parser.add_argument('--max-edit-dist', type=int, default=25,
                        help='Maximal edit distance between sentences')
    parser.add_argument('--min-alpha-ratio', type=float, default=0.65,
                        help='Minimal length of sentences')
    args = parser.parse_args()

    install_dependencies()
    main(Path(args.dataset), Parameters(args))
