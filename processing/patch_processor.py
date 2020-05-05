import re
import sys
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional, Iterable
from diff_match_patch import patch_obj, diff_match_patch
from nltk import sent_tokenize
from nltk.translate.bleu_score import sentence_bleu, SmoothingFunction
import ray

from .patch import merge_patches, invert_patches
from .tools.latex2text import LatexMarkupProcessor
from cosmas.generated.cosmas_pb2 import Patch


SIMILARITY_DISTANCE = 10
TIMESTAMP_DISTANCE = 3000  # milliseconds


def group_similar_patches_by_distance(patches: List[patch_obj]) -> List[List[patch_obj]]:
    i, j = 0, 0
    similar_patches = []
    while i < len(patches):
        patch1 = patches[i]
        begin1, end1 = patch1.start2, patch1.start2 + patch1.length2
        while j < len(patches):
            patch2 = patches[j]
            begin2, end2 = patch2.start1, patch2.start1 + patch2.length1
            distance = max(begin2 - end1, begin1 - end2)
            if distance > SIMILARITY_DISTANCE:
                break
            begin1 = min(begin1, patch2.start2)
            end1 = max(begin2, patch2.start2 + patch2.length2)
            j += 1
        similar_patches.append(patches[i:j])
        i, j = j, j + 1
    return similar_patches


def group_similar_patches_by_timestamps(patches: List[patch_obj], timestamps: List[int]) -> List[List[patch_obj]]:
    i, j = 0, 1
    similar_patches = []
    while i < len(patches):
        while j < len(patches) and timestamps[j - 1] - timestamps[j] <= TIMESTAMP_DISTANCE:
            j += 1
        similar_patches.append(patches[i:j])
        i, j = j, j + 1
    return similar_patches


def extract_one_diff(text_before: str, text_after: str) -> Optional[Tuple[str, str]]:
    sents_before = list(filter(lambda sent: len(sent) >= 5, sent_tokenize(text_before)))
    sents_after = list(filter(lambda sent: len(sent) >= 5, sent_tokenize(text_after)))

    prefix_len = 0
    while prefix_len < min(len(sents_before), len(sents_after)) and sents_before[prefix_len] == sents_after[prefix_len]:
        prefix_len += 1
    if prefix_len == min(len(sents_before), len(sents_after)):
        return None

    suffix_len = 0
    while suffix_len < min(len(sents_before), len(sents_after)) and sents_before[-suffix_len - 1] == sents_after[-suffix_len - 1]:
        suffix_len += 1
    if suffix_len == min(len(sents_before), len(sents_after)):
        return None

    if suffix_len == 0:
        return ' '.join(sents_before[prefix_len:]), ' '.join(sents_after[prefix_len:])
    return ' '.join(sents_before[prefix_len:-suffix_len]), ' '.join(sents_after[prefix_len:-suffix_len])


def extract_multiple_diffs(text_before: str, text_after: str) -> List[Tuple[str, str]]:
    sents_before = list(filter(lambda sent: len(sent) >= 5, sent_tokenize(text_before)))
    sents_after = list(filter(lambda sent: len(sent) >= 5, sent_tokenize(text_after)))

    n, m = len(sents_before), len(sents_after)
    if abs(n - m) > 10:
        return []

    chencherry = SmoothingFunction()

    i = 0
    diffs = []
    while i < n:
        has_equal = False
        for j in range(i - 10, i + 10):
            if j < 0 or j >= m:
                continue
            if sents_before[i] == sents_after[j]:
                has_equal = True

        if has_equal:
            continue

        best_j, best_bleu = None, None
        for j in range(i - 10, i + 10):
            if j < 0 or j >= m:
                continue

            # noinspection PyTypeChecker
            bleu_score = sentence_bleu([sents_before[i]], sents_after[j], smoothing_function=chencherry.method1)
            if best_bleu is None or bleu_score > best_bleu:
                best_bleu = bleu_score
                best_j = j

        if best_j is None:
            i += 1
            continue

        best_spans, best_bleu = None, None
        for di in range(2):
            if i + di >= n:
                continue

            sent_before = ' '.join(sents_before[i:i + di + 1])
            for dj in range(-1, 2):
                if best_j + dj < 0 or best_j + dj >= m:
                    continue

                j1, j2 = min(best_j, best_j + dj), max(best_j, best_j + dj)
                sent_after = ' '.join(sents_after[j1:j2 + 1])

                # noinspection PyTypeChecker
                bleu_score = sentence_bleu([sent_before], sent_after, smoothing_function=chencherry.method1)
                if best_bleu is None or bleu_score > best_bleu:
                    best_bleu = bleu_score
                    best_spans = ((i, i + di + 1), (j1, j2 + 1))

        ((i1, i2), (j1, j2)) = best_spans
        diffs.append((' '.join(sents_before[i1:i2]), ' '.join(sents_after[j1:j2])))
        i = i2

    return diffs


class DiffExtractor(ABC):
    @abstractmethod
    def extract_diff(self, text_before: str, text_after: str):
        pass

    @abstractmethod
    def get_diffs(self) -> Iterable[Tuple[str, str]]:
        pass


@ray.remote
class OneDiffExtractor(DiffExtractor):
    def __init__(self):
        self.markup_processor = LatexMarkupProcessor()
        self.diffs = []

    def extract_diff(self, text_before: str, text_after: str):
        text_before = self.markup_processor.remove_markup(text_before)
        text_after = self.markup_processor.remove_markup(text_after)
        diff = extract_one_diff(text_before, text_after)
        if diff:
            self.diffs.append(diff)

    def get_diffs(self) -> Iterable[Tuple[str, str]]:
        return self.diffs.copy()


@ray.remote
class MultipleDiffExtractor(DiffExtractor):
    def __init__(self):
        self.markup_processor = LatexMarkupProcessor()
        self.diffs = []

    def extract_diff(self, text_before: str, text_after: str):
        text_before = self.markup_processor.remove_markup(text_before)
        text_after = self.markup_processor.remove_markup(text_after)
        diffs = extract_multiple_diffs(text_before, text_after)
        self.diffs.extend(diffs)

    def get_diffs(self) -> Iterable[Tuple[str, str]]:
        return self.diffs.copy()


class AdvancedPatchProcessor:
    def __init__(self, num_cpus):
        self.patcher = diff_match_patch()
        self.bibtex_regex = re.compile(r'@(article|book|conference|inproceedings|masterthesis|online|phdthesis|techreport|unpublished)')

        ray.init(num_cpus=num_cpus)
        self.num_cpus = num_cpus
        # self.actors = [OneDiffExtractor.remote() for _ in range(num_cpus)]
        self.actors = [MultipleDiffExtractor.remote() for _ in range(num_cpus)]
        self.index = 0

    def process_patches(self, text: str, patches: List[Patch]):
        if self.bibtex_regex.search(text) is not None:
            return

        patcher = diff_match_patch()

        patch_objs, timestamps = [], []
        for patch in patches:
            new_patch_objs = patcher.patch_fromText(patch.text)
            patch_objs.extend(new_patch_objs)
            timestamps.extend(patch.timestamp for _ in new_patch_objs)

        inverted_patch_objs = invert_patches(patch_objs)
        timestamps.reverse()

        similar_patch_objs = group_similar_patches_by_timestamps(inverted_patch_objs, timestamps)

        for patch_group in similar_patch_objs:
            text_before = self.patcher.patch_apply(patch_group, text)[0]
            self.actors[self.index % self.num_cpus].extract_diff.remote(text_before, text)
            self.index += 1
            text = text_before

    def get_diffs(self) -> Iterable[Tuple[str, str]]:
        results = ray.get([actor.get_diffs.remote() for actor in self.actors])
        for result in results:
            for diff in result:
                yield diff


class SimplePatchProcessor:
    def __init__(self):
        self.patcher = diff_match_patch()
        self.eos_regex = re.compile(r'([.?!;])')
        self.total_successes = 0
        self.total_errors = 0

        self.equation1_regex = re.compile(r'\$\$.*?\$\$')
        self.equation2_regex = re.compile(r'(\\begin\{equation.*?\}.*?\\end\{equation.*?\}|\$.*?\$|\\\[.*?\\\])')
        self.equation3_regex = re.compile(r'(^.*?(\\end\{equation\}|\\\])|(\\begin\{equation\}|\\\[).*?$)')
        self.comment_regex = re.compile(r'(^|[^\\])%.*?(\n|$)')
        self.percent_regex = re.compile(r'\\%')
        self.quote_regex = re.compile(r'[\'"`]')
        self.prerequisite_regex = re.compile(r'(\\document.*?(\n|$)|\\usepackage.*?(\n|$)|\\(re)?newcommand.*?(\n|$)|\\let.*?(\n|$))')
        self.label_regex = re.compile(r'\\?label\{(.*?)\}')
        self.cite_regex = re.compile(r'\\?cite.*?\{(.*?)\}')
        self.href_regex = re.compile(r'\\href\{.*?\}\{.*?\}')
        self.textcolor_regex = re.compile(r'\\?textcolor\{.*?\}\{(.*?)\}')
        self.text_regex = re.compile(r'\\?text.*?\{(.*?)\}')
        self.includegraphics_regex = re.compile(r'\\includegraphics(\[.*?\])?\{.*?\}')
        self.url_regex = re.compile(r'\\url(\[.*?\])?\{.*?\}')
        self.section_regex = re.compile(r'\\(sub)*section\{.*?\}')
        self.braces_regex = re.compile(r'\{(.*?)\}')
        self.cmd_regex = re.compile(r'\\[^\\\s]+\s')
        self.spaces_regex = re.compile('(\s|~)+')

        self.diffs = []

    @staticmethod
    def _determine_spans(text: str, patches: List[patch_obj]) -> Tuple[Tuple[int, int], Tuple[int, int]]:
        text_len = len(text)

        before_span_begin_dist = patches[0].start1
        before_span_end_dist = text_len - (patches[0].start1 + patches[0].length1)

        after_span_begin_dist = patches[0].start2
        after_span_end_dist = (text_len - patches[0].length1 + patches[0].length2) - (patches[0].start2 + patches[0].length2)

        for prev_patch, patch in zip(patches[:-1], patches[1:]):
            text_len = text_len - prev_patch.length1 + prev_patch.length2

            before_span_begin_dist = min(before_span_begin_dist, patch.start1)
            after_span_begin_dist = min(after_span_begin_dist, patch.start2)

            before_span_end_dist = min(before_span_end_dist, text_len - (patch.start1 + patch.length1))
            after_span_end_dist = min(after_span_end_dist, (text_len - patch.length1 + patch.length2) - (patch.start2 + patch.length2))

        text_len = text_len - patches[-1].length1 + patches[-1].length2
        before_span = (before_span_begin_dist, len(text) - before_span_end_dist)
        after_span = (after_span_begin_dist, text_len - after_span_end_dist)

        return before_span, after_span

    def _extract_sentence(self, text: str, ind_l: int, ind_r: int) -> Tuple[str, int]:
        try:
            if len(text) == 0:
                return '', 0
            while ind_l >= 0:
                if not self.eos_regex.match(text[ind_l]):
                    ind_l -= 1
                elif ind_l + 1 < len(text) and text[ind_l + 1].isalnum():
                    ind_l -= 1
                elif ind_l - 3 >= 0 and text[ind_l - 3:ind_l] in ['i.e', 'e.g', 'etc']:
                    ind_l -= 1
                elif ind_l - 5 >= 0 and text[ind_l - 5:ind_l] in ['et al']:
                    ind_l -= 1
                else:
                    break
            while ind_r + 1 < len(text):
                if not self.eos_regex.match(text[ind_r]):
                    ind_r += 1
                elif text[ind_r + 1].isalnum():
                    ind_r += 1
                elif ind_r - 3 >= 0 and text[ind_r - 3:ind_r] in ['i.e', 'e.g', 'etc']:
                    ind_r += 1
                elif ind_r - 5 >= 0 and text[ind_r - 5:ind_r] in ['et al']:
                    ind_r += 1
                else:
                    break
            sentence = text[ind_l + 1:ind_r + 1]
            return sentence.strip(), 0
        except:
            return '', 1

    def _normalize_sentence(self, text: str) -> str:
        text = self.equation1_regex.sub(' _MATH_ ', text)
        text = self.equation2_regex.sub(' _MATH_ ', text)
        text = self.equation3_regex.sub(' _MATH_ ', text)
        text = self.comment_regex.sub(r' ', text)
        text = self.percent_regex.sub(r' % ', text)
        text = self.quote_regex.sub(r'\'', text)
        text = self.prerequisite_regex.sub(r' ', text)
        text = self.label_regex.sub(r' ', text)
        text = self.cite_regex.sub(r' _REF_ ', text)
        text = self.href_regex.sub(' ', text)
        text = self.textcolor_regex.sub(r' \1 ', text)
        text = self.text_regex.sub(r' \1 ', text)
        text = self.includegraphics_regex.sub(r' ', text)
        text = self.url_regex.sub(r' ', text)
        text = self.section_regex.sub(r' ', text)
        text = self.braces_regex.sub(r' \1 ', text)
        text = self.cmd_regex.sub(r' ', text)
        text = text.replace('\n', ' ').strip()
        text = self.spaces_regex.sub(r' ', text)
        return text

    def process_patches(self, text: str, patches: List[Patch]):
        patcher = diff_match_patch()

        patch_objs = []
        for patch in patches:
            patch_objs.extend(patcher.patch_fromText(patch.text))

        inverted_patch_objs = invert_patches(patch_objs)

        similar_patch_objs = group_similar_patches_by_distance(inverted_patch_objs)

        edited_pieces = []
        for patch_group in similar_patch_objs:
            before_span, after_span = self._determine_spans(text, patch_group)
            new_text = self.patcher.patch_apply(patch_group, text)[0]

            piece_before, err0 = self._extract_sentence(text, before_span[0], before_span[1])
            piece_after, err1 = self._extract_sentence(new_text, after_span[0], after_span[1])

            self.total_successes += 2 - err0 - err1
            self.total_errors += err0 + err1

            if err0 == 0 and err1 == 0:
                edited_pieces.append((
                    piece_before,
                    piece_after
                ))
            text = new_text

        self.diffs.extend((self._normalize_sentence(text_after), self._normalize_sentence(text_before))
                          for text_before, text_after in reversed(edited_pieces))

    def get_diffs(self) -> Iterable[Tuple[str, str]]:
        return self.diffs.copy()

    def print_statistics(self):
        print(f'successes: {self.total_successes}, errors: {self.total_errors}, error_rate: {self.total_errors / self.total_successes:.6f}', file=sys.stderr)
