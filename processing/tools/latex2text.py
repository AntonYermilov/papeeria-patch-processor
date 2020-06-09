import re
from typing import Optional, List


class LatexEnv:
    def __init__(self, regex: str, args: Optional[str] = None, repl: Optional[str] = None, use_raw_regex: bool = False):
        if use_raw_regex:
            self.regex = regex
        else:
            self.regex = r'\\begin\{' + regex + '\}'
            if args:
                for arg in args:
                    if arg == 'A':
                        self.regex += r'\{.*?\}'
                    elif arg == 'O':
                        self.regex += r'(\[.*?\])?'
                    elif args == 'P':
                        self.regex += r'\[.*?\]'
                    else:
                        raise ValueError('unsupported options for args: only "A", "O" and "P" are available')
            self.regex += r'(.*?)'
            self.regex += r'\\end\{' + regex + r'\}'
        self.regex = re.compile(self.regex, re.DOTALL | re.IGNORECASE)
        self.repl = repl

    def replace_all(self, text: str) -> str:
        if self.repl is None:
            return text
        return self.regex.sub(self.repl, text)


class EnvList:
    def __init__(self, envs: List[LatexEnv]):
        self.envs = envs

    def replace_all(self, text: str, apply_once: bool = True) -> str:
        converged = False
        while not converged:
            new_text = text
            for env in self.envs:
                new_text = env.replace_all(new_text)
            converged = apply_once or new_text == text
            text = new_text
        return text


class Prerequisites(EnvList):
    def __init__(self):
        super().__init__([
            LatexEnv(r'^.*?\\begin\{document\}', args=None, repl='', use_raw_regex=True),
            LatexEnv(r'\\end\{document\}.*?$', args=None, repl='', use_raw_regex=True)
        ])


class Equations(EnvList):
    def __init__(self, replacement=' MATH '):
        self.replacement = replacement
        super().__init__([
            LatexEnv(r'align', args=None, repl=replacement),
            LatexEnv(r'align\*', args=None, repl=replacement),
            LatexEnv(r'alignat', args='A', repl=replacement),
            LatexEnv(r'alignat\*', args='A', repl=replacement),
            LatexEnv(r'displaymath', args=None, repl=replacement),
            LatexEnv(r'equation', args=None, repl=replacement),
            LatexEnv(r'equation\*', args=None, repl=replacement),
            LatexEnv(r'eqnarray', args=None, repl=replacement),
            LatexEnv(r'eqnarray\*', args=None, repl=replacement),
            LatexEnv(r'flalign', args=None, repl=replacement),
            LatexEnv(r'flalign\*', args=None, repl=replacement),
            LatexEnv(r'multline', args=None, repl=replacement),
            LatexEnv(r'multline\*', args=None, repl=replacement),
            LatexEnv(r'\$\$.*?\$\$', args=None, repl=replacement, use_raw_regex=True),
            LatexEnv(r'\\\[.*?\\\]', args=None, repl=replacement, use_raw_regex=True),
            LatexEnv(r'\$.*?\$', args=None, repl=replacement, use_raw_regex=True),
            LatexEnv(r'\\\(.*?\\\)', args=None, repl=replacement, use_raw_regex=True)
        ])


class Cites(EnvList):
    def __init__(self, replacement=' CITE '):
        self.replacement = replacement
        super().__init__([
            LatexEnv(r'\\cite(\[.*?\])?\{.*?\}', args=None, repl=replacement, use_raw_regex=True),
            LatexEnv(r'\\citet(\[.*?\])?\{.*?\}', args=None, repl=replacement, use_raw_regex=True),
            LatexEnv(r'\\citep(\[.*?\])?\{.*?\}', args=None, repl=replacement, use_raw_regex=True)
        ])


class Headings(EnvList):
    def __init__(self):
        super().__init__([
            LatexEnv(r'\\title\*?\{(.*?)\}', args=None, repl=r' \1. ', use_raw_regex=True),
            LatexEnv(r'\\chapter\*?\{(.*?)\}', args=None, repl=r' \1. ', use_raw_regex=True),
            LatexEnv(r'\\part\*?\{(.*?)\}', args=None, repl=r' \1. ', use_raw_regex=True),
            LatexEnv(r'\\section\*?\{(.*?)\}', args=None, repl=r' \1. ', use_raw_regex=True),
            LatexEnv(r'\\subsection\*?\{(.*?)\}', args=None, repl=r' \1. ', use_raw_regex=True),
            LatexEnv(r'\\subsubsection\*?\{(.*?)\}', args=None, repl=r' \1. ', use_raw_regex=True),
            LatexEnv(r'\\paragraph\*?\{(.*?)\}', args=None, repl=r' \1. ', use_raw_regex=True),
        ])


class Figures(EnvList):
    def __init__(self, replacement=' FIGURE '):
        self.replacement = replacement
        super().__init__([
            LatexEnv(r'figure', args=None, repl=replacement),
            LatexEnv(r'figure\*', args=None, repl=replacement),
            LatexEnv(r'tikzpicture', args=None, repl=replacement),
            LatexEnv(r'tikzpicture\*', args=None, repl=replacement)
        ])


class Tables(EnvList):
    def __init__(self, replacement=' TABLE '):
        self.replacement = replacement
        super().__init__([
            LatexEnv(r'table', args=None, repl=replacement),
            LatexEnv(r'table\*', args=None, repl=replacement),
            LatexEnv(r'tabular', args=None, repl=replacement),
            LatexEnv(r'tabular\*', args=None, repl=replacement)
        ])


class Texts(EnvList):
    def __init__(self):
        super().__init__([
            LatexEnv(r'\\text\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\textbf\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\tbf\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\textit\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\tot\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\texttt\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\ttt\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\textsc\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\tsc\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\emph\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\textcolor\{.*?\}\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\text[a-z]*\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\footnote\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
        ])

    def replace_all(self, text: str, apply_once: bool = True) -> str:
        return super().replace_all(text, False)


class Urls(EnvList):
    def __init__(self):
        super().__init__([
            LatexEnv(r'\\url\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\href\{.*?\}\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
        ])


class Comments(EnvList):
    def __init__(self):
        super().__init__([
            LatexEnv(r'([^\\])%.*?(\n|$)', args=None, repl=r'\1\n', use_raw_regex=True)
        ])

    def replace_all(self, text: str, apply_once: bool = True) -> str:
        return super().replace_all(text, False)


class Commands(EnvList):
    def __init__(self):
        super().__init__([
            LatexEnv(r'\\(eq|foot)?ref\{.*?\}', args=None, repl=r' REF ', use_raw_regex=True),
            LatexEnv(r'\\rule\{.*?\}\{.*?\}', args=None, repl=r' ', use_raw_regex=True),
            LatexEnv(r'\\label\{.*?\}', args=None, repl=r' ', use_raw_regex=True),
            LatexEnv(r'\\paragraph\*?\{.*?\}', args=None, repl=r' ', use_raw_regex=True),
            LatexEnv(r'\\pagenumbering(\{.*?\})+', args=None, repl=r' ', use_raw_regex=True),
            LatexEnv(r'\\setcounter(\{.*?\})+', args=None, repl=r' ', use_raw_regex=True),
            LatexEnv(r'\\[A-Za-z]+([ ~\n])', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\todo(\{.*?\})?', args=None, repl=r' ', use_raw_regex=True),
            LatexEnv(r'\\bibliography\{.*?\}', args=None, repl=r' ', use_raw_regex=True),
            LatexEnv(r'\\(text|textbf|textit|texttt|textsc|tbf|tit|ttt|tsc)', args=None, repl=r' ', use_raw_regex=True),
        ])

    def replace_all(self, text: str, apply_once: bool = True) -> str:
        return super().replace_all(text, False)


class OtherEnvs(EnvList):
    def __init__(self):
        super().__init__([
            LatexEnv(r'\\begin\{(.*?)\}(.*?)\\end\{\1\}', args=None, repl=r' \2 ', use_raw_regex=True),
            LatexEnv(r'\\item\[(.*?)\]', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\item', args=None, repl=r' ', use_raw_regex=True),
            LatexEnv(r'[fF][iI][gG]\.', args=None, repl=r' figure ', use_raw_regex=True),
            LatexEnv(r'\\includegraphics\[.*?\]\{.*?\}', args=None, repl=r' ', use_raw_regex=True),
            LatexEnv(r'\\input\{.*?\}', args=None, repl=r' ', use_raw_regex=True),
            LatexEnv(r'\\underline\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\centering\{(.*?)\}', args=None, repl=r' \1 ', use_raw_regex=True),
            LatexEnv(r'\\[a-zA-Z]+(\[.*?\]|\{.*?\})*?', args=None, repl=r' ', use_raw_regex=True)
        ])

    def replace_all(self, text: str, apply_once: bool = True) -> str:
        return super().replace_all(text, False)


class LatexMarkupProcessor:
    def __init__(self):
        self.env_processors = [
            Prerequisites(),
            Equations(),
            Cites(),
            Headings(),
            Figures(),
            Tables(),
            Urls(),
            Texts(),
            Commands(),
            OtherEnvs(),
            Comments(),
            # add footnotes, hrefs/urls, ...
        ]
        # self.alphabet_regex = re.compile(rf'[^{alphabet}]', re.DOTALL | re.IGNORECASE)
        self.spaces_regex = re.compile('(\s|~)+', re.DOTALL | re.IGNORECASE)

    def remove_markup(self, text: str) -> str:
        text = text.replace('\\\\', '\n')  # remove latex-style newlines
        text = text.replace('\\$', ' ')  # remove dollar signs

        for env_processor in self.env_processors:
            text = env_processor.replace_all(text)

        text = text.replace('\\%', '%')

        text = text.replace('``', '"')
        text = text.replace('\'\'', '"')
        text = text.replace('`', '\'')

        text = text.replace('\n\n', '\n.\n')  # add dot separators on blank lines

        # text = self.alphabet_regex.sub(r' ', text)
        text = self.spaces_regex.sub(r' ', text)
        return text
