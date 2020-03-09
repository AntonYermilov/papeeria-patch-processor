import sys
import re
import json
import argparse
from pathlib import Path
from tqdm import tqdm
from nltk import sent_tokenize


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
    except:
        print('Unable to download some of dependencies, check your internet connection', file=sys.stderr)
        return False


def main(content_dir: Path, local_path: str):
    """
    Prepares wiki dataset from the output of WikiExtractor
    """
    quote_regex = re.compile(r'[\'"`]')
    sent_regex = re.compile(r'^[A-Z][a-zA-Z!@#№*()\[\]{}\-_+=;:\',.<>?/ ]*[.?!;]$')

    with Path(local_path).open('w') as outp:
        for i, source_file in enumerate(tqdm(content_dir.rglob('*'))):
            if not source_file.is_file():
                continue
            with source_file.open('r') as src:
                for line in src.readlines():
                    if not line:
                        continue
                    content = json.loads(line)
                    text = content['text']

                    for sent in sent_tokenize(text):
                        sent = sent.strip()
                        sent = quote_regex.sub(r'\'', sent)
                        if len(sent) >= 20 and sent_regex.fullmatch(sent):
                            print(sent, file=outp)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--content-dir', type=str, default='/home/eranik/wiki/en_wiki_json/')
    parser.add_argument('--local-path', type=str, default='/home/eranik/wiki/en_wiki.txt')
    args = parser.parse_args()
    install_dependencies()
    main(Path(args.content_dir), args.local_path)
