import sys
import re
from typing import List, Tuple
from tqdm import tqdm
from langdetect import detect

from .metrics import char_edit_distance, word_edit_distance, latin_alphabet_ratio


class SentencePair:
    def __init__(self, source_sent: str, target_sent: str, perplexity_scorer=None):
        self.source_sent = source_sent
        self.target_sent = target_sent
        self.char_distance = char_edit_distance(source_sent, target_sent, no_digits=True, summarized=True)
        self.alpha_ratio = min(latin_alphabet_ratio(source_sent), latin_alphabet_ratio(target_sent))

        word_distance = word_edit_distance(source_sent, target_sent, summarized=False)
        self.word_substitutions = word_distance[0]
        self.word_insertions = word_distance[1]
        self.word_deletions = word_distance[2]

        self.source_perplexity = 0 if perplexity_scorer is None else perplexity_scorer.perplexity(source_sent)
        self.target_perplexity = 0 if perplexity_scorer is None else perplexity_scorer.perplexity(target_sent)

    def print_formatted(self):
        print(f'source sentence: {self.source_sent}\n'
              f'target sentence: {self.target_sent}\n'
              f'metrics: char_levenshtein={self.char_distance}, alpha_ratio={self.alpha_ratio:.3f}, '
              f'S={self.word_substitutions}, I={self.word_insertions}, D={self.word_deletions}, '
              f'source_ppl={self.source_perplexity}, target_ppl={self.target_perplexity}\n')


def is_probably_english(sent: str):
    try:
        return detect(sent) == 'en'
    except:
        return False


def select_sentence_pairs(sentence_pairs: List[Tuple[str, str]], sent_regex: str = None,
                          min_length: int = None, max_length: int = None,
                          min_char_levenshtein: int = None, max_char_levenshtein: int = None,
                          min_alpha_ratio: float = None) -> List[SentencePair]:
    if not min_length:
        min_length = 0
    if not max_length:
        max_length = int(1e9)
    if not min_char_levenshtein:
        min_char_levenshtein = -1
    if not max_char_levenshtein:
        max_char_levenshtein = int(1e9)
    if not min_alpha_ratio:
        min_alpha_ratio = 0.0

    print(f'Filtering sentence pairs by length [{min_length}, {max_length}]', file=sys.stderr)
    sentence_pairs = filter(lambda pair: min_length <= min(len(pair[0]), len(pair[1])), sentence_pairs)
    sentence_pairs = filter(lambda pair: max(len(pair[0]), len(pair[1])) <= max_length, sentence_pairs)

    if sent_regex:
        print(f'Filtering sentence pairs with regex: {sent_regex}', file=sys.stderr)
        sent_regex = re.compile(sent_regex)
        sentence_pairs = filter(lambda pair: sent_regex.fullmatch(pair[0]) and sent_regex.fullmatch(pair[1]), sentence_pairs)

    sentence_pairs = map(lambda pair: SentencePair(*pair), sentence_pairs)

    print(f'Filtering sentence pairs by levenshtein distance [{min_char_levenshtein}, {max_char_levenshtein}]', file=sys.stderr)
    sentence_pairs = filter(lambda sp: min_char_levenshtein <= sp.char_distance <= max_char_levenshtein, sentence_pairs)

    print(f'Filtering sentence pairs by alphabetic symbols ratio >= {min_alpha_ratio}', file=sys.stderr)
    sentence_pairs = filter(lambda sp: sp.alpha_ratio >= min_alpha_ratio, sentence_pairs)

    print('Filtering english sentences', file=sys.stderr)
    sentence_pairs = filter(lambda sp: is_probably_english(sp.source_sent) or is_probably_english(sp.target_sent), sentence_pairs)

    sentence_pairs = list(tqdm(sentence_pairs))

    print('Done', file=sys.stderr)
    return sentence_pairs
