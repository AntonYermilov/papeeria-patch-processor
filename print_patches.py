import argparse
from pathlib import Path
from urllib.parse import unquote
from cosmas.generated.cosmas_pb2 import PatchList


def process_patch(patch_path: Path):
    if not patch_path.is_file():
        return
    try:
        patch_list = PatchList()
        patch_list.ParseFromString(patch_path.read_bytes())

        if len(patch_list.patches) > 3:
            return

        print(f'File {patch_path}:')
        for patch in patch_list.patches:
            text = unquote(patch.text)
            print(text)
    except:
        pass


def main():
    """
    Prints patches from the specified directory to the standard output
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('path', type=str)
    args = parser.parse_args()

    path = Path(args.path)

    process_patch(path)
    for file in path.rglob('./*'):
        process_patch(file)


if __name__ == '__main__':
    main()
