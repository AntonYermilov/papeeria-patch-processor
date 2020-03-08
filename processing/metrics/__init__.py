import numpy as np
from nltk import word_tokenize

from .levenshtein import levenshtein


def char_edit_distance(sent1: str, sent2: str, no_digits=False, summarized=True):
    if no_digits:
        sent1 = filter(lambda c: not str.isdigit(c), sent1)
        sent2 = filter(lambda c: not str.isdigit(c), sent2)
    sent1 = np.array(list(map(ord, sent1)), dtype=np.int32)
    sent2 = np.array(list(map(ord, sent2)), dtype=np.int32)

    result = levenshtein(sent1, sent2)
    if summarized:
        result = sum(result)
    return result


def word_edit_distance(sent1: str, sent2: str, summarized=True):
    words1 = word_tokenize(sent1)
    words2 = word_tokenize(sent2)
    all_words = set(words1) | set(words2)
    encoder = {word: i for i, word in enumerate(all_words)}

    words1 = np.array(list(map(lambda w: encoder[w], words1)), dtype=np.int32)
    words2 = np.array(list(map(lambda w: encoder[w], words2)), dtype=np.int32)

    result = levenshtein(words1, words2)
    if summarized:
        result = sum(result)
    return result


def latin_alphabet_ratio(sent: str) -> float:
    return sum(map(str.isalpha, sent)) / (len(sent) + 0.5)
