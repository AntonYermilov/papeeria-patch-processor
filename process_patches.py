import sys
from pathlib import Path
from cosmas.generated.cosmas_pb2 import PatchList
from diff_match_patch import diff_match_patch
from processing.patch_processor import PatchProcessor
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


def main():
    content_dir = Path('resources', 'content')
    patches_dir = Path('resources', 'patches')

    processor = PatchProcessor()
    sentence_pairs = []

    for doc_id in content_dir.iterdir():
        doc_id = doc_id.name

        text_path = content_dir / doc_id
        patches_path = patches_dir / doc_id

        content = None
        for file in text_path.rglob('*'):
            content = file.read_text()
        patches = []
        for file in patches_path.rglob('./*'):
            patch_list = PatchList()
            patch_list.ParseFromString(file.read_bytes())
            patches.extend(patch_list.patches)

        patches.sort(key=lambda k: k.timestamp)

        patch_objs = []
        patcher = diff_match_patch()
        for patch in patches:
            patch_objs.extend(patcher.patch_fromText(patch.text))

        edited_pieces = processor.process_patches(content, patch_objs)
        sentence_pairs.extend(edited_pieces)

    processor.print_statistics()

    sentence_pairs = select_sentence_pairs(sentence_pairs, sent_regex=r'^[A-Z][^%$\\&^~]*$',
                                           min_length=20, max_length=500,
                                           min_char_levenshtein=1, max_char_levenshtein=20,
                                           min_alpha_ratio=0.65)
    for sp in sentence_pairs:
        sp.print_formatted()


if __name__ == '__main__':
    install_dependencies()
    main()
