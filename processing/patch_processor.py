import re
import sys
from typing import List, Tuple
from diff_match_patch import patch_obj, diff_match_patch
from .patch import merge_patches, invert_patches


class PatchProcessor:
    SIMILARITY_DISTANCE = 20

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
        self.spaces_regex = re.compile('\s+')

    @staticmethod
    def _group_similar_patches(patches: List[patch_obj]) -> List[List[patch_obj]]:
        i, j = 0, 0
        similar_patches = []
        while i < len(patches):
            patch1 = patches[i]
            begin1, end1 = patch1.start2, patch1.start2 + patch1.length2
            while j < len(patches):
                patch2 = patches[j]
                begin2, end2 = patch2.start1, patch2.start1 + patch2.length1
                distance = max(begin2 - end1, begin1 - end2)
                if distance > PatchProcessor.SIMILARITY_DISTANCE:
                    break
                begin1 = min(begin1, patch2.start2)
                end1 = max(begin2, patch2.start2 + patch2.length2)
                j += 1
            similar_patches.append(patches[i:j])
            i, j = j, j + 1
        return similar_patches

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
        text = self.cite_regex.sub(r' _CITE_ ', text)
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

    def process_patches(self, text: str, patches: List[patch_obj]) -> List[Tuple[str, str]]:
        patches = invert_patches(patches)

        edited_pieces = []
        similar_patches = self._group_similar_patches(patches)

        for patch_group in similar_patches:
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

        edited_pieces = [(self._normalize_sentence(text_after), self._normalize_sentence(text_before))
                         for text_before, text_after in reversed(edited_pieces)]
        return edited_pieces

    def print_statistics(self):
        print(f'successes: {self.total_successes}, errors: {self.total_errors}, error_rate: {self.total_errors / self.total_successes:.6f}', file=sys.stderr)

